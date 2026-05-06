defmodule Mezzanine.CoordinationEngine.TraceDataset.Receipt do
  @moduledoc "Trace to eval/replay dataset conversion receipt."

  @enforce_keys [
    :receipt_ref,
    :fixture_refs,
    :coordination_run_ref,
    :tenant_ref,
    :eval_dataset_ref,
    :replay_dataset_ref,
    :drift_run_ref,
    :memory_evidence_refs,
    :context_budget_refs,
    :guardrail_evidence_refs,
    :cost_ledger_refs,
    :appkit_projection_refs,
    :aitrace_span_refs,
    :trace_refs,
    :replay_refs,
    :store_tier_refs,
    :persistence_profile_ref,
    :local_restart_safe_profile_ref,
    :retention_ref,
    :debug_tap_posture_ref,
    :redaction_posture,
    :status
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{}
end

defmodule Mezzanine.CoordinationEngine.TraceDataset do
  @moduledoc """
  Converts coordination run trace refs into eval and replay dataset refs.
  """

  alias Mezzanine.CoordinationEngine.{Run, TraceDataset.Receipt, Validation}

  @fixture_refs ["AOC-027", "AOC-032", "PERSIST-AOC-004", "PERSIST-AOC-005"]
  @required_strings [
    :eval_dataset_ref,
    :replay_dataset_ref,
    :drift_run_ref,
    :persistence_profile_ref,
    :local_restart_safe_profile_ref,
    :retention_ref,
    :debug_tap_posture_ref
  ]
  @required_lists [
    :memory_evidence_refs,
    :context_budget_refs,
    :guardrail_evidence_refs,
    :cost_ledger_refs,
    :appkit_projection_refs,
    :aitrace_span_refs,
    :store_tier_refs
  ]

  @spec from_run(Run.t(), map()) :: {:ok, Receipt.t()} | {:error, term()}
  def from_run(%Run{} = run, attrs) when is_map(attrs) do
    with :ok <- Validation.reject_raw(attrs),
         :ok <- required_strings(attrs),
         :ok <- required_lists(attrs) do
      {:ok,
       %Receipt{
         receipt_ref: "trace-dataset://" <> run.spec.coordination_run_ref,
         fixture_refs: @fixture_refs,
         coordination_run_ref: run.spec.coordination_run_ref,
         tenant_ref: run.spec.tenant_ref,
         eval_dataset_ref: Validation.fetch(attrs, :eval_dataset_ref),
         replay_dataset_ref: Validation.fetch(attrs, :replay_dataset_ref),
         drift_run_ref: Validation.fetch(attrs, :drift_run_ref),
         memory_evidence_refs: ref_list(attrs, :memory_evidence_refs),
         context_budget_refs: ref_list(attrs, :context_budget_refs),
         guardrail_evidence_refs: ref_list(attrs, :guardrail_evidence_refs),
         cost_ledger_refs: ref_list(attrs, :cost_ledger_refs),
         appkit_projection_refs: ref_list(attrs, :appkit_projection_refs),
         aitrace_span_refs: ref_list(attrs, :aitrace_span_refs),
         trace_refs: run.trace_refs,
         replay_refs: run.replay_refs,
         store_tier_refs: ref_list(attrs, :store_tier_refs),
         persistence_profile_ref: Validation.fetch(attrs, :persistence_profile_ref),
         local_restart_safe_profile_ref: Validation.fetch(attrs, :local_restart_safe_profile_ref),
         retention_ref: Validation.fetch(attrs, :retention_ref),
         debug_tap_posture_ref: Validation.fetch(attrs, :debug_tap_posture_ref),
         redaction_posture: :refs_only,
         status: :pass
       }}
    end
  end

  def from_run(_run, _attrs), do: {:error, :invalid_trace_dataset_projection}

  defp required_strings(attrs) do
    Enum.reduce_while(@required_strings, :ok, fn field, :ok ->
      case Validation.require_binary(attrs, field) do
        {:ok, _value} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp required_lists(attrs) do
    Enum.reduce_while(@required_lists, :ok, fn field, :ok ->
      case Validation.require_string_list(attrs, field) do
        {:ok, _value} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp ref_list(attrs, field) do
    {:ok, values} = Validation.require_string_list(attrs, field)
    values
  end
end
