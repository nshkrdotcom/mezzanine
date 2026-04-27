defmodule Mezzanine.AgentRuntime.AgentRunSpec do
  @moduledoc """
  M2 intake contract below AppKit.

  The contract carries refs, profile slots, and bounded policy maps only. Raw
  prompts, provider payloads, model selectors, tool-call maps, credentials, and
  host paths are rejected by the shared S0 unsafe-field guard.
  """

  alias Mezzanine.AgentRuntime.{ProfileBundle, ProfileSlotRef, Support}

  @required [
    :tenant_ref,
    :installation_ref,
    :profile_ref,
    :subject_ref,
    :run_ref,
    :trace_id,
    :idempotency_key,
    :objective,
    :runtime_profile_ref,
    :tool_catalog_ref,
    :authority_context_ref,
    :memory_profile_ref,
    :artifact_policy_ref,
    :max_turns,
    :timeout_policy
  ]
  @optional [
    :profile_bundle,
    :source_ref,
    :session_ref,
    :workspace_ref,
    :worker_ref,
    :initial_context_refs,
    :review_policy_ref,
    :operator_interrupt_policy_ref,
    :release_manifest_ref
  ]
  @fields @required ++ @optional
  @enforce_keys @required
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(%__MODULE__{} = spec), do: {:ok, spec}

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- reject_unknown(attrs, @fields),
         :ok <- Support.reject_unsafe(attrs, :invalid_agent_run_spec),
         true <-
           required_refs?(attrs, [
             :tenant_ref,
             :installation_ref,
             :profile_ref,
             :subject_ref,
             :run_ref,
             :trace_id,
             :objective,
             :tool_catalog_ref,
             :authority_context_ref,
             :artifact_policy_ref
           ]),
         true <- present_binary?(Support.required(attrs, :idempotency_key)),
         runtime_profile_ref <- Support.required(attrs, :runtime_profile_ref),
         true <- ProfileSlotRef.valid_ref?(:runtime_profile_ref, runtime_profile_ref),
         memory_profile_ref <- Support.required(attrs, :memory_profile_ref),
         true <- ProfileSlotRef.valid_ref?(:memory_profile_ref, memory_profile_ref),
         max_turns <- Support.required(attrs, :max_turns),
         true <- is_integer(max_turns) and max_turns > 0,
         timeout_policy <- Support.required(attrs, :timeout_policy),
         true <- is_map(timeout_policy),
         {:ok, profile_bundle} <-
           optional_profile_bundle(Support.optional(attrs, :profile_bundle)),
         true <-
           optional_refs?(attrs, [
             :source_ref,
             :session_ref,
             :workspace_ref,
             :worker_ref,
             :review_policy_ref,
             :operator_interrupt_policy_ref,
             :release_manifest_ref
           ]),
         initial_context_refs <- Support.optional(attrs, :initial_context_refs, []),
         true <-
           is_list(initial_context_refs) and Enum.all?(initial_context_refs, &Support.safe_ref?/1) do
      {:ok,
       struct!(
         __MODULE__,
         attrs
         |> values(@fields)
         |> Map.put(:profile_bundle, profile_bundle)
         |> Map.put(:initial_context_refs, initial_context_refs)
       )}
    else
      _ -> {:error, :invalid_agent_run_spec}
    end
  end

  def new(_attrs), do: {:error, :invalid_agent_run_spec}
  def new!(attrs), do: bang(new(attrs))
  def dump(%__MODULE__{} = spec), do: dump_struct(spec)

  defp optional_profile_bundle(nil), do: {:ok, nil}
  defp optional_profile_bundle(%ProfileBundle{} = bundle), do: {:ok, bundle}
  defp optional_profile_bundle(attrs), do: ProfileBundle.new(attrs)

  defp required_refs?(attrs, keys),
    do: Enum.all?(keys, &(Support.required(attrs, &1) |> Support.safe_ref?()))

  defp optional_refs?(attrs, keys),
    do: Enum.all?(keys, fn key -> optional_ref?(Support.optional(attrs, key)) end)

  defp optional_ref?(nil), do: true
  defp optional_ref?(value), do: Support.safe_ref?(value)

  defp present_binary?(value), do: is_binary(value) and String.trim(value) != ""
  defp values(attrs, fields), do: Map.new(fields, &{&1, Support.optional(attrs, &1)})
  defp bang({:ok, value}), do: value
  defp bang({:error, reason}), do: raise(ArgumentError, Atom.to_string(reason))

  defp reject_unknown(attrs, fields) do
    allowed = MapSet.new(Enum.flat_map(fields, &[&1, Atom.to_string(&1)]))

    if Enum.all?(Map.keys(attrs), &MapSet.member?(allowed, &1)),
      do: :ok,
      else: {:error, :unknown_fields}
  end

  defp dump_struct(%_{} = struct) do
    struct
    |> Map.from_struct()
    |> Support.dump_value()
    |> Support.drop_nil_values()
  end
end

defmodule Mezzanine.AgentRuntime.AgentTurnState do
  @moduledoc "Replay-safe durable state for one M2 turn."

  alias Mezzanine.AgentRuntime.Support

  @states [
    :initialized,
    :context_ready,
    :action_planned,
    :awaiting_authority,
    :action_submitted,
    :awaiting_observation,
    :observed,
    :memory_pending,
    :completed,
    :blocked,
    :failed,
    :cancelled
  ]
  @terminal_states [:completed, :blocked, :failed, :cancelled]
  @required [:turn_ref, :run_ref, :subject_ref, :turn_index, :state, :started_at, :trace_id]
  @fields @required ++
            [
              :ended_at,
              :context_refs,
              :reflection_ref,
              :planned_action_ref,
              :tool_action_request_ref,
              :tool_action_receipt_ref,
              :authority_decision_ref,
              :observation_ref,
              :semantic_fact_refs,
              :memory_commit_ref,
              :terminal_reason,
              :causation_id,
              :snapshot_epoch,
              :budget_before,
              :budget_after
            ]
  @enforce_keys @required
  defstruct @fields

  @type t :: %__MODULE__{}

  def states, do: @states
  def terminal_states, do: @terminal_states

  def new(%__MODULE__{} = turn), do: {:ok, turn}

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- reject_unknown(attrs, @fields),
         :ok <- Support.reject_unsafe(attrs, :invalid_agent_turn_state),
         true <- required_refs?(attrs, [:turn_ref, :run_ref, :subject_ref, :trace_id]),
         turn_index <- Support.required(attrs, :turn_index),
         true <- is_integer(turn_index) and turn_index > 0,
         state <- normalize_atom(Support.required(attrs, :state)),
         true <- state in @states,
         true <- timestamp?(Support.required(attrs, :started_at)),
         true <- optional_timestamp?(Support.optional(attrs, :ended_at)),
         context_refs <- Support.optional(attrs, :context_refs, []),
         true <- list_of_refs?(context_refs),
         semantic_fact_refs <- Support.optional(attrs, :semantic_fact_refs, []),
         true <- list_of_refs?(semantic_fact_refs),
         true <-
           optional_refs?(attrs, [
             :reflection_ref,
             :planned_action_ref,
             :tool_action_request_ref,
             :tool_action_receipt_ref,
             :authority_decision_ref,
             :observation_ref,
             :memory_commit_ref,
             :causation_id
           ]),
         snapshot_epoch <- Support.optional(attrs, :snapshot_epoch, 0),
         true <- is_integer(snapshot_epoch) and snapshot_epoch >= 0,
         true <- optional_map?(Support.optional(attrs, :budget_before)),
         true <- optional_map?(Support.optional(attrs, :budget_after)) do
      {:ok,
       struct!(
         __MODULE__,
         attrs
         |> values(@fields)
         |> Map.put(:state, state)
         |> Map.put(:turn_index, turn_index)
         |> Map.put(:context_refs, context_refs)
         |> Map.put(:semantic_fact_refs, semantic_fact_refs)
         |> Map.put(:snapshot_epoch, snapshot_epoch)
       )}
    else
      _ -> {:error, :invalid_agent_turn_state}
    end
  end

  def new(_attrs), do: {:error, :invalid_agent_turn_state}
  def new!(attrs), do: bang(new(attrs))
  def dump(%__MODULE__{} = turn), do: dump_struct(turn)

  def transition(%__MODULE__{} = turn, next_state) do
    next_state = normalize_atom(next_state)

    cond do
      next_state == turn.state ->
        {:ok, turn}

      turn.state in @terminal_states ->
        {:error, :invalid_agent_turn_transition}

      state_index(next_state) > state_index(turn.state) ->
        {:ok, %{turn | state: next_state}}

      true ->
        {:error, :invalid_agent_turn_transition}
    end
  end

  defp state_index(state), do: Enum.find_index(@states, &(&1 == state)) || -1
  defp normalize_atom(value) when is_binary(value), do: String.to_existing_atom(value)
  defp normalize_atom(value), do: value

  defp required_refs?(attrs, keys),
    do: Enum.all?(keys, &(Support.required(attrs, &1) |> Support.safe_ref?()))

  defp optional_refs?(attrs, keys),
    do: Enum.all?(keys, &(Support.optional(attrs, &1) |> optional_ref?()))

  defp optional_ref?(nil), do: true
  defp optional_ref?(value), do: Support.safe_ref?(value)
  defp list_of_refs?(values), do: is_list(values) and Enum.all?(values, &Support.safe_ref?/1)
  defp optional_map?(nil), do: true
  defp optional_map?(value), do: is_map(value)
  defp timestamp?(%DateTime{}), do: true
  defp timestamp?(value), do: Support.safe_ref?(value)
  defp optional_timestamp?(nil), do: true
  defp optional_timestamp?(value), do: timestamp?(value)
  defp values(attrs, fields), do: Map.new(fields, &{&1, Support.optional(attrs, &1)})
  defp bang({:ok, value}), do: value
  defp bang({:error, reason}), do: raise(ArgumentError, Atom.to_string(reason))

  defp reject_unknown(attrs, fields) do
    allowed = MapSet.new(Enum.flat_map(fields, &[&1, Atom.to_string(&1)]))

    if Enum.all?(Map.keys(attrs), &MapSet.member?(allowed, &1)),
      do: :ok,
      else: {:error, :unknown_fields}
  end

  defp dump_struct(%_{} = struct),
    do: struct |> Map.from_struct() |> Support.dump_value() |> Support.drop_nil_values()
end

defmodule Mezzanine.AgentRuntime.ToolActionRequest do
  @moduledoc "One governed M2 tool action request."

  alias Mezzanine.AgentRuntime.Support

  @required [
    :action_ref,
    :turn_ref,
    :run_ref,
    :profile_ref,
    :tool_ref,
    :capability_ref,
    :input_artifact_ref,
    :authority_context_ref,
    :idempotency_key,
    :trace_id
  ]
  @fields @required ++
            [
              :workspace_ref,
              :deadline_at,
              :expected_output_schema_ref,
              :operator_confirmation_ref
            ]
  @enforce_keys @required
  defstruct @fields

  def new(%__MODULE__{} = request), do: {:ok, request}

  def new(attrs) when is_map(attrs) or is_list(attrs),
    do: build(__MODULE__, attrs, :invalid_tool_action_request, @fields, @required)

  def new(_attrs), do: {:error, :invalid_tool_action_request}
  def new!(attrs), do: bang(new(attrs))
  def dump(%__MODULE__{} = request), do: dump_struct(request)

  defp build(module, attrs, error, fields, required) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- reject_unknown(attrs, fields),
         :ok <- Support.reject_unsafe(attrs, error),
         true <- Enum.all?(required, &(Support.required(attrs, &1) |> required_value?())),
         true <- optional_refs?(attrs, fields -- required) do
      {:ok, struct!(module, values(attrs, fields))}
    else
      _ -> {:error, error}
    end
  end

  defp required_value?(value), do: Support.safe_ref?(value) or present_binary?(value)
  defp present_binary?(value), do: is_binary(value) and String.trim(value) != ""

  defp optional_refs?(attrs, keys),
    do: Enum.all?(keys, &(Support.optional(attrs, &1) |> optional_ref?()))

  defp optional_ref?(nil), do: true
  defp optional_ref?(%DateTime{}), do: true
  defp optional_ref?(value), do: Support.safe_ref?(value)
  defp values(attrs, fields), do: Map.new(fields, &{&1, Support.optional(attrs, &1)})
  defp bang({:ok, value}), do: value
  defp bang({:error, reason}), do: raise(ArgumentError, Atom.to_string(reason))

  defp reject_unknown(attrs, fields) do
    allowed = MapSet.new(Enum.flat_map(fields, &[&1, Atom.to_string(&1)]))

    if Enum.all?(Map.keys(attrs), &MapSet.member?(allowed, &1)),
      do: :ok,
      else: {:error, :unknown_fields}
  end

  defp dump_struct(%_{} = struct),
    do: struct |> Map.from_struct() |> Support.dump_value() |> Support.drop_nil_values()
end

defmodule Mezzanine.AgentRuntime.ToolActionReceipt do
  @moduledoc "Observed result of one M2 tool action."

  alias Mezzanine.AgentRuntime.Support

  @statuses [
    :succeeded,
    :failed,
    :denied,
    :approval_required,
    :skipped,
    "succeeded",
    "failed",
    "denied",
    "approval_required",
    "skipped"
  ]
  @required [:receipt_ref, :action_ref, :turn_ref, :status, :trace_id]
  @fields @required ++
            [
              :lower_receipt_ref,
              :output_artifact_refs,
              :evidence_refs,
              :error_class,
              :retry_posture,
              :redaction_ref
            ]
  @enforce_keys @required
  defstruct @fields

  def new(%__MODULE__{} = receipt), do: {:ok, receipt}

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- reject_unknown(attrs, @fields),
         :ok <- Support.reject_unsafe(attrs, :invalid_tool_action_receipt),
         true <- required_refs?(attrs, [:receipt_ref, :action_ref, :turn_ref, :trace_id]),
         status <- Support.required(attrs, :status),
         true <- status in @statuses,
         true <- optional_refs?(attrs, [:lower_receipt_ref, :redaction_ref]),
         output_artifact_refs <- Support.optional(attrs, :output_artifact_refs, []),
         true <- list_of_refs?(output_artifact_refs),
         evidence_refs <- Support.optional(attrs, :evidence_refs, []),
         true <- list_of_refs?(evidence_refs) do
      {:ok,
       struct!(
         __MODULE__,
         attrs
         |> values(@fields)
         |> Map.put(:status, normalize_atom(status))
         |> Map.put(:output_artifact_refs, output_artifact_refs)
         |> Map.put(:evidence_refs, evidence_refs)
       )}
    else
      _ -> {:error, :invalid_tool_action_receipt}
    end
  end

  def new(_attrs), do: {:error, :invalid_tool_action_receipt}
  def new!(attrs), do: bang(new(attrs))
  def dump(%__MODULE__{} = receipt), do: dump_struct(receipt)

  defp required_refs?(attrs, keys),
    do: Enum.all?(keys, &(Support.required(attrs, &1) |> Support.safe_ref?()))

  defp optional_refs?(attrs, keys),
    do: Enum.all?(keys, &(Support.optional(attrs, &1) |> optional_ref?()))

  defp optional_ref?(nil), do: true
  defp optional_ref?(value), do: Support.safe_ref?(value)
  defp list_of_refs?(values), do: is_list(values) and Enum.all?(values, &Support.safe_ref?/1)
  defp normalize_atom(value) when is_binary(value), do: String.to_existing_atom(value)
  defp normalize_atom(value), do: value
  defp values(attrs, fields), do: Map.new(fields, &{&1, Support.optional(attrs, &1)})
  defp bang({:ok, value}), do: value
  defp bang({:error, reason}), do: raise(ArgumentError, Atom.to_string(reason))

  defp reject_unknown(attrs, fields) do
    allowed = MapSet.new(Enum.flat_map(fields, &[&1, Atom.to_string(&1)]))

    if Enum.all?(Map.keys(attrs), &MapSet.member?(allowed, &1)),
      do: :ok,
      else: {:error, :unknown_fields}
  end

  defp dump_struct(%_{} = struct),
    do: struct |> Map.from_struct() |> Support.dump_value() |> Support.drop_nil_values()
end

defmodule Mezzanine.AgentRuntime.AgentLoopCommand do
  @moduledoc "Command envelope for M2 turn approval/input/control signals."

  alias Mezzanine.AgentRuntime.Support

  @kinds [
    :approve,
    :deny,
    :submit_turn,
    :cancel,
    :replan,
    :rework,
    "approve",
    "deny",
    "submit_turn",
    "cancel",
    "replan",
    "rework"
  ]
  @fields [
    :command_ref,
    :command_kind,
    :run_ref,
    :actor_ref,
    :idempotency_key,
    :payload_ref,
    :trace_id
  ]
  @enforce_keys @fields
  defstruct @fields

  def new(%__MODULE__{} = command), do: {:ok, command}

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- reject_unknown(attrs, @fields),
         :ok <- Support.reject_unsafe(attrs, :invalid_agent_loop_command),
         true <-
           required_refs?(attrs, [:command_ref, :run_ref, :actor_ref, :payload_ref, :trace_id]),
         command_kind <- Support.required(attrs, :command_kind),
         true <- command_kind in @kinds,
         idempotency_key <- Support.required(attrs, :idempotency_key),
         true <- is_binary(idempotency_key) and String.trim(idempotency_key) != "" do
      {:ok,
       struct!(
         __MODULE__,
         values(attrs, @fields) |> Map.put(:command_kind, normalize_atom(command_kind))
       )}
    else
      _ -> {:error, :invalid_agent_loop_command}
    end
  end

  def new(_attrs), do: {:error, :invalid_agent_loop_command}
  def new!(attrs), do: bang(new(attrs))
  def dump(%__MODULE__{} = command), do: dump_struct(command)

  defp required_refs?(attrs, keys),
    do: Enum.all?(keys, &(Support.required(attrs, &1) |> Support.safe_ref?()))

  defp normalize_atom(value) when is_binary(value), do: String.to_existing_atom(value)
  defp normalize_atom(value), do: value
  defp values(attrs, fields), do: Map.new(fields, &{&1, Support.optional(attrs, &1)})
  defp bang({:ok, value}), do: value
  defp bang({:error, reason}), do: raise(ArgumentError, Atom.to_string(reason))

  defp reject_unknown(attrs, fields) do
    allowed = MapSet.new(Enum.flat_map(fields, &[&1, Atom.to_string(&1)]))

    if Enum.all?(Map.keys(attrs), &MapSet.member?(allowed, &1)),
      do: :ok,
      else: {:error, :unknown_fields}
  end

  defp dump_struct(%_{} = struct),
    do: struct |> Map.from_struct() |> Support.dump_value() |> Support.drop_nil_values()
end

defmodule Mezzanine.AgentRuntime.AgentLoopProjection do
  @moduledoc "M2 run projection reduced into M1-readable rows."

  alias Mezzanine.AgentRuntime.{
    AgentTurnState,
    RuntimeCommandResult,
    RuntimeEventRow,
    Support,
    ToolActionReceipt,
    ToolActionRequest
  }

  @required [:run_ref, :subject_ref, :workflow_ref, :terminal_state, :status]
  @fields @required ++
            [
              :current_turn_ref,
              :turn_states,
              :action_requests,
              :action_receipts,
              :runtime_events,
              :command_results,
              :budget_state,
              :candidate_fact_refs,
              :memory_commit_refs,
              :memory_proof_refs,
              :session_ref,
              :workspace_ref,
              :worker_ref,
              :receipt_ref_set,
              :diagnostics
            ]
  @enforce_keys @required
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(%__MODULE__{} = projection), do: {:ok, projection}

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- reject_unknown(attrs, @fields),
         :ok <- Support.reject_unsafe(attrs, :invalid_agent_loop_projection),
         true <- required_refs?(attrs, [:run_ref, :subject_ref, :workflow_ref]),
         true <- present_binary?(Support.required(attrs, :terminal_state)),
         true <- present_binary?(Support.required(attrs, :status)),
         true <-
           optional_refs?(attrs, [:current_turn_ref, :session_ref, :workspace_ref, :worker_ref]),
         {:ok, turn_states} <-
           nested_list(Support.optional(attrs, :turn_states, []), AgentTurnState),
         {:ok, action_requests} <-
           nested_list(Support.optional(attrs, :action_requests, []), ToolActionRequest),
         {:ok, action_receipts} <-
           nested_list(Support.optional(attrs, :action_receipts, []), ToolActionReceipt),
         {:ok, runtime_events} <-
           nested_list(Support.optional(attrs, :runtime_events, []), RuntimeEventRow),
         {:ok, command_results} <-
           nested_list(Support.optional(attrs, :command_results, []), RuntimeCommandResult),
         budget_state <- Support.optional(attrs, :budget_state, %{}),
         true <- is_map(budget_state),
         candidate_fact_refs <- Support.optional(attrs, :candidate_fact_refs, []),
         true <- list_of_refs?(candidate_fact_refs),
         memory_commit_refs <- Support.optional(attrs, :memory_commit_refs, []),
         true <- list_of_refs?(memory_commit_refs),
         memory_proof_refs <- Support.optional(attrs, :memory_proof_refs, []),
         true <- list_of_refs?(memory_proof_refs),
         receipt_ref_set <- Support.optional(attrs, :receipt_ref_set, %{}),
         true <- ref_set?(receipt_ref_set),
         diagnostics <- Support.optional(attrs, :diagnostics, []),
         true <- is_list(diagnostics) do
      {:ok,
       struct!(
         __MODULE__,
         values(attrs, @fields)
         |> Map.put(:turn_states, turn_states)
         |> Map.put(:action_requests, action_requests)
         |> Map.put(:action_receipts, action_receipts)
         |> Map.put(:runtime_events, runtime_events)
         |> Map.put(:command_results, command_results)
         |> Map.put(:budget_state, budget_state)
         |> Map.put(:candidate_fact_refs, candidate_fact_refs)
         |> Map.put(:memory_commit_refs, memory_commit_refs)
         |> Map.put(:memory_proof_refs, memory_proof_refs)
         |> Map.put(:receipt_ref_set, receipt_ref_set)
         |> Map.put(:diagnostics, diagnostics)
       )}
    else
      _ -> {:error, :invalid_agent_loop_projection}
    end
  end

  def new(_attrs), do: {:error, :invalid_agent_loop_projection}
  def new!(attrs), do: bang(new(attrs))
  def dump(%__MODULE__{} = projection), do: dump_struct(projection)

  defp nested_list(values, module) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case module.new(value) do
        {:ok, struct} -> {:cont, {:ok, [struct | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp nested_list(_values, _module), do: {:error, :invalid_nested}

  defp required_refs?(attrs, keys),
    do: Enum.all?(keys, &(Support.required(attrs, &1) |> Support.safe_ref?()))

  defp optional_refs?(attrs, keys),
    do: Enum.all?(keys, &(Support.optional(attrs, &1) |> optional_ref?()))

  defp optional_ref?(nil), do: true
  defp optional_ref?(value), do: Support.safe_ref?(value)
  defp list_of_refs?(values), do: is_list(values) and Enum.all?(values, &Support.safe_ref?/1)

  defp ref_set?(value) when value == %{}, do: true

  defp ref_set?(value) when is_map(value) do
    Enum.all?(value, fn
      {_key, refs} when is_list(refs) -> Enum.all?(refs, &Support.safe_ref?/1)
      {_key, ref} -> Support.safe_ref?(ref)
    end)
  end

  defp ref_set?(_value), do: false

  defp present_binary?(value), do: is_binary(value) and String.trim(value) != ""
  defp values(attrs, fields), do: Map.new(fields, &{&1, Support.optional(attrs, &1)})
  defp bang({:ok, value}), do: value
  defp bang({:error, reason}), do: raise(ArgumentError, Atom.to_string(reason))

  defp reject_unknown(attrs, fields) do
    allowed = MapSet.new(Enum.flat_map(fields, &[&1, Atom.to_string(&1)]))

    if Enum.all?(Map.keys(attrs), &MapSet.member?(allowed, &1)),
      do: :ok,
      else: {:error, :unknown_fields}
  end

  defp dump_struct(%_{} = struct),
    do: struct |> Map.from_struct() |> Support.dump_value() |> Support.drop_nil_values()
end
