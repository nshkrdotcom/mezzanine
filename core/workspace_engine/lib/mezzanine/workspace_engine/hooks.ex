defmodule Mezzanine.WorkspaceEngine.Hooks do
  @moduledoc """
  Fail-closed workspace hook execution contracts.

  The workspace engine owns hook selection, timeout posture, and receipts. The
  concrete hook implementation is supplied by the caller so lower process and
  remote placement mechanics stay outside this neutral package.
  """

  alias Mezzanine.WorkspaceEngine.WorkspaceRecord

  @default_timeout_ms 30_000
  @default_max_output_bytes 4_096
  @redaction_marker "[REDACTED]"
  @default_action_by_stage %{
    after_create: :halt,
    before_run: :halt,
    after_run: :continue,
    before_remove: :halt,
    prepare_workspace: :halt,
    after_turn: :continue
  }
  @stage_atoms_by_name Map.new(Map.keys(@default_action_by_stage), fn stage ->
                         {Atom.to_string(stage), stage}
                       end)

  @spec run(WorkspaceRecord.t(), atom() | String.t(), keyword()) ::
          {:ok, [map()]}
          | {:error, {:hook_failed | :hook_timeout | :unknown_hook_stage, map()}}
  def run(%WorkspaceRecord{} = workspace, stage, opts \\ []) when is_list(opts) do
    runner = Keyword.get(opts, :runner, &default_runner/2)

    with {:ok, stage} <- known_stage(stage) do
      if skip_stage?(workspace, stage) do
        {:ok, []}
      else
        workspace.hook_specs
        |> Enum.map(&normalize_hook/1)
        |> Enum.filter(&(&1.stage == stage))
        |> run_hooks(workspace, runner, opts)
      end
    end
  end

  defp skip_stage?(%WorkspaceRecord{created_now?: false}, :after_create), do: true
  defp skip_stage?(_workspace, _stage), do: false

  defp run_hooks(hooks, workspace, runner, opts) do
    Enum.reduce_while(hooks, {:ok, []}, fn hook, {:ok, receipts} ->
      case execute_hook(hook, workspace, runner, opts) do
        {:ok, receipt} -> {:cont, {:ok, receipts ++ [receipt]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp execute_hook(hook, workspace, runner, opts) do
    context = context(workspace, hook)
    timeout_ms = timeout_ms(hook)

    task = Task.async(fn -> safe_run(runner, hook, context) end)

    hook_result =
      case Task.yield(task, timeout_ms) do
        nil -> Task.shutdown(task, :brutal_kill)
        result -> result
      end

    case hook_result do
      {:ok, {:runner_result, {:ok, result}}} ->
        {:ok, receipt(hook, :succeeded, result, nil, :continue, opts)}

      {:ok, {:runner_result, :ok}} ->
        {:ok, receipt(hook, :succeeded, %{}, nil, :continue, opts)}

      {:ok, {:runner_result, {:error, reason}}} ->
        hook_error(:hook_failed, hook, :failed, reason, opts)

      {:ok, {:runner_result, other}} ->
        hook_error(:hook_failed, hook, :failed, {:invalid_hook_result, other}, opts)

      {:ok, {:runner_exit, kind, reason}} ->
        hook_error(:hook_failed, hook, :failed, {:hook_exit, kind, reason}, opts)

      nil ->
        hook_error(:hook_timeout, hook, :timed_out, :timeout, opts)
    end
  end

  defp safe_run(runner, hook, context) do
    {:runner_result, runner.(hook, context)}
  catch
    kind, reason -> {:runner_exit, kind, reason}
  end

  defp default_runner(_hook, _context), do: :ok

  defp context(%WorkspaceRecord{} = workspace, hook) do
    runtime_refs = workspace.remote_hints || %{}

    %{
      workspace_id: workspace.workspace_id,
      workspace_ref: "workspace://#{workspace.workspace_id}",
      installation_id: workspace.installation_id,
      subject_id: workspace.subject_id,
      subject_ref: workspace.subject_ref,
      run_ref: value(runtime_refs, :run_ref),
      workflow_ref: value(runtime_refs, :workflow_ref),
      attempt_ref: value(runtime_refs, :attempt_ref),
      logical_ref: workspace.logical_ref,
      cwd: workspace.concrete_path,
      concrete_path: workspace.concrete_path,
      stage: hook.stage,
      created_now?: workspace.created_now?,
      reuse?: workspace.reuse?,
      env_refs: hook.env_refs
    }
  end

  defp hook_error(kind, hook, status, reason, opts) do
    action = hook.action_on_failure
    receipt = receipt(hook, status, %{}, reason, action, opts)

    case action do
      :halt -> {:error, {kind, receipt}}
      :continue -> {:ok, receipt}
    end
  end

  defp receipt(hook, status, result, reason, action, opts) do
    {result, result_truncation} = sanitize(result, opts)
    {reason, reason_truncation} = sanitize(reason, opts)

    %{
      hook_ref: hook.hook_ref,
      stage: hook.stage,
      status: status,
      fatal?: hook.fatal?,
      action: action,
      timeout_ms: hook.timeout_ms,
      result: result,
      reason: reason,
      truncated?: truncated?(result_truncation, reason_truncation)
    }
  end

  defp normalize_hook(hook) when is_map(hook) do
    stage = normalize_stage(value(hook, :stage))
    action_on_failure = action_on_failure(hook, stage)

    %{
      hook_ref:
        first_value(
          [value(hook, :hook_ref), value(hook, :ref), value(hook, :name)],
          stage_ref(stage)
        ),
      stage: stage,
      timeout_ms: timeout_ms(hook),
      attrs: hook_attrs(hook),
      env_refs:
        ref_list(
          first_value(
            [value(hook, :env_refs), value(hook, :environment_refs), value(hook, :env_ref)],
            []
          )
        ),
      fatal?: fatal_flag(action_on_failure),
      action_on_failure: action_on_failure
    }
  end

  defp normalize_hook(hook) when is_atom(hook) do
    action_on_failure = action_on_failure(%{}, hook)

    %{
      hook_ref: Atom.to_string(hook),
      stage: hook,
      timeout_ms: @default_timeout_ms,
      attrs: %{},
      env_refs: [],
      fatal?: fatal_flag(action_on_failure),
      action_on_failure: action_on_failure
    }
  end

  defp hook_attrs(hook) do
    attrs = value(hook, :attrs, %{}) || %{}

    case value(hook, :command) do
      command when is_binary(command) and command != "" ->
        Map.put_new(attrs, "command", command)

      _missing ->
        attrs
    end
  end

  defp timeout_ms(hook) do
    case value(hook, :timeout_ms) do
      timeout when is_integer(timeout) and timeout > 0 -> timeout
      _other -> @default_timeout_ms
    end
  end

  defp known_stage(stage) do
    case normalize_stage(stage) do
      stage ->
        case Map.fetch(@default_action_by_stage, stage) do
          {:ok, _action} -> {:ok, stage}
          :error -> {:error, {:unknown_hook_stage, %{stage: stage}}}
        end
    end
  end

  defp normalize_stage(stage) when is_atom(stage), do: stage

  defp normalize_stage(stage) when is_binary(stage),
    do: Map.get(@stage_atoms_by_name, stage, stage)

  defp normalize_stage(stage), do: stage

  defp action_on_failure(hook, stage) do
    case value(hook, :on_error) do
      on_error when on_error in [:continue, "continue"] ->
        :continue

      on_error when on_error in [:halt, "halt", :fail_closed, "fail_closed"] ->
        :halt

      _other ->
        default_action_on_failure(stage)
    end
  end

  defp default_action_on_failure(stage),
    do: Map.get(@default_action_by_stage, normalize_stage(stage), :halt)

  defp fatal_flag(:halt), do: true
  defp fatal_flag(_action), do: false

  defp stage_ref(stage) when is_atom(stage), do: Atom.to_string(stage)
  defp stage_ref(stage), do: to_string(stage)

  defp ref_list(values) when is_list(values), do: Enum.map(values, &to_string/1)
  defp ref_list(value) when is_binary(value) and value != "", do: [value]
  defp ref_list(_value), do: []

  defp first_value(values, default) do
    Enum.find(values, default, fn
      nil -> false
      _value -> true
    end)
  end

  defp sanitize(value, opts) when is_map(value) do
    Enum.reduce(value, {%{}, :complete}, fn {key, nested}, {acc, truncation} ->
      {safe_nested, nested_truncation} = sanitize(nested, opts)
      {Map.put(acc, key, safe_nested), combine_truncation(truncation, nested_truncation)}
    end)
  end

  defp sanitize(values, opts) when is_list(values) do
    Enum.map_reduce(values, :complete, fn value, truncation ->
      {safe_value, value_truncation} = sanitize(value, opts)
      {safe_value, combine_truncation(truncation, value_truncation)}
    end)
  end

  defp sanitize(value, opts) when is_binary(value) do
    value
    |> redact(redactions(opts))
    |> truncate(max_output_bytes(opts))
  end

  defp sanitize(value, _opts), do: {value, :complete}

  defp combine_truncation(:truncated, _other), do: :truncated
  defp combine_truncation(_current, :truncated), do: :truncated
  defp combine_truncation(_current, _other), do: :complete

  defp truncated?(:truncated, _reason), do: true
  defp truncated?(_result, :truncated), do: true
  defp truncated?(_result, _reason), do: false

  defp redact(value, redactions) do
    Enum.reduce(redactions, value, fn redaction, acc ->
      String.replace(acc, redaction, @redaction_marker)
    end)
  end

  defp redactions(opts) do
    opts
    |> Keyword.get(:redactions, [])
    |> Enum.filter(fn
      value when is_binary(value) -> value != ""
      _other -> false
    end)
  end

  defp max_output_bytes(opts) do
    case Keyword.get(opts, :max_output_bytes, @default_max_output_bytes) do
      value when is_integer(value) and value > 0 -> value
      _other -> @default_max_output_bytes
    end
  end

  defp truncate(value, max_bytes) when byte_size(value) <= max_bytes, do: {value, :complete}

  defp truncate(value, max_bytes) do
    truncated =
      value
      |> String.graphemes()
      |> Enum.reduce_while("", fn grapheme, acc ->
        next = acc <> grapheme

        if byte_size(next) <= max_bytes do
          {:cont, next}
        else
          {:halt, acc}
        end
      end)

    {truncated, :truncated}
  end

  defp value(attrs, key, default \\ nil) when is_atom(key) do
    case Map.fetch(attrs, key) do
      {:ok, value} ->
        value

      :error ->
        case Map.fetch(attrs, Atom.to_string(key)) do
          {:ok, value} -> value
          :error -> default
        end
    end
  end
end
