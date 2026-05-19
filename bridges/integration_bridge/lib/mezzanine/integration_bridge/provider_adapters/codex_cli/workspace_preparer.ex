defmodule Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.WorkspacePreparer do
  @moduledoc false

  alias Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.Support
  alias Mezzanine.WorkspaceEngine.{ExecutionPlaneCommandRunner, Hooks, WorkspaceRecord}

  @spec prepare(String.t() | nil, keyword()) :: :ok | {:error, term()}
  def prepare(workspace_root, opts) when is_binary(workspace_root) do
    prepare_workspace_fun = Keyword.get(opts, :prepare_workspace_fun, &File.mkdir_p/1)
    prepare_workspace_fun.(workspace_root)
  end

  def prepare(_workspace_root, _opts), do: :ok

  @spec for_hooks(map(), keyword(), String.t()) :: WorkspaceRecord.t() | nil
  def for_hooks(attrs, opts, workspace_root) do
    hook_specs = hook_specs(attrs, opts)

    if hook_specs == [] do
      nil
    else
      opts
      |> Keyword.get(:workspace_record)
      |> workspace_record(attrs, workspace_root, hook_specs)
    end
  end

  @spec run_hooks(WorkspaceRecord.t() | nil, atom(), keyword()) ::
          {:ok, list()} | {:error, term()}
  def run_hooks(nil, _stage, _opts), do: {:ok, []}

  def run_hooks(%WorkspaceRecord{} = workspace, stage, opts) do
    Hooks.run(workspace, stage,
      runner: hook_runner(opts),
      redactions: Keyword.get(opts, :hook_redactions, []),
      max_output_bytes: Keyword.get(opts, :hook_max_output_bytes, 4_096)
    )
  end

  @spec run_after_run_hooks(WorkspaceRecord.t() | nil, keyword()) :: list()
  def run_after_run_hooks(workspace, opts) do
    case run_hooks(workspace, :after_run, opts) do
      {:ok, receipts} -> receipts
      {:error, {_reason, receipt}} -> [receipt]
    end
  end

  defp hook_specs(attrs, opts) do
    opts
    |> Keyword.get(:workspace_hook_specs, Support.map_value(attrs, :workspace_hook_specs))
    |> case do
      nil -> Support.map_value(attrs, :hook_specs)
      specs -> specs
    end
    |> List.wrap()
    |> Enum.reject(&is_nil/1)
  end

  defp hook_runner(opts) do
    Keyword.get(
      opts,
      :workspace_hook_runner,
      ExecutionPlaneCommandRunner.runner(env: Keyword.get(opts, :workspace_hook_env, %{}))
    )
  end

  defp workspace_record(%WorkspaceRecord{} = record, _attrs, _workspace_root, hook_specs),
    do: %{record | hook_specs: hook_specs}

  defp workspace_record(_record, attrs, workspace_root, hook_specs) do
    subject_ref = Support.map_value(attrs, :subject_ref)
    installation_id = Support.map_value(attrs, :installation_ref) || "installation://unknown"
    subject_id = subject_ref || "subject://unknown"
    workspace_id = workspace_id(attrs, workspace_root)

    %WorkspaceRecord{
      workspace_id: workspace_id,
      installation_id: installation_id,
      subject_id: subject_id,
      subject_ref: subject_ref,
      logical_ref: "workspace:#{installation_id}:#{subject_id}",
      concrete_root: workspace_root,
      concrete_path: workspace_root,
      slug: Support.ref_suffix(workspace_id),
      placement_kind: :local,
      cleanup_policy: :never,
      safety_hash: Support.digest([workspace_id, workspace_root]),
      file_scope: %{writable_roots: [workspace_root], read_roots: [workspace_root]},
      hook_specs: hook_specs,
      remote_hints: %{
        run_ref: Support.map_value(attrs, :run_ref),
        workflow_ref:
          "workflow://codex-agent-runtime/#{Support.ref_suffix(Support.map_value(attrs, :run_ref))}"
      },
      created_now?: false,
      reuse?: true
    }
  end

  defp workspace_id(attrs, workspace_root) do
    case Support.map_value(attrs, :workspace_ref) do
      "workspace://" <> id when id != "" -> id
      value when is_binary(value) and value != "" -> value
      _missing -> "codex-agent-runtime-#{Support.ref_suffix(workspace_root)}"
    end
  end
end
