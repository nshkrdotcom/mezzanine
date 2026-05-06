defmodule Mezzanine.OptimizationEngine.EvaluatorPool.Lease do
  @moduledoc "Ref-only evaluator batch lease."

  @type t :: %__MODULE__{owner_ref: String.t(), expires_at_ref: String.t()}
  @enforce_keys [:owner_ref, :expires_at_ref]
  defstruct @enforce_keys
end

defmodule Mezzanine.OptimizationEngine.EvaluatorPool.Batch do
  @moduledoc "Leased evaluator batch."

  alias Mezzanine.OptimizationEngine.EvaluatorPool.Lease

  @type t :: %__MODULE__{
          batch_ref: String.t(),
          run_ref: String.t(),
          candidate_ref: String.t(),
          example_refs: [String.t()],
          lease: Lease.t(),
          idempotency_key: String.t(),
          retry_policy_ref: String.t(),
          cost_refs: [String.t()],
          trace_refs: [String.t()]
        }

  @enforce_keys [
    :batch_ref,
    :run_ref,
    :candidate_ref,
    :example_refs,
    :lease,
    :idempotency_key,
    :retry_policy_ref,
    :cost_refs,
    :trace_refs
  ]
  defstruct @enforce_keys
end

defmodule Mezzanine.OptimizationEngine.EvaluatorPool.Result do
  @moduledoc "Normalized evaluator result."

  @type t :: %__MODULE__{
          batch_ref: String.t(),
          idempotency_key: String.t(),
          eval_ref: String.t(),
          score: float(),
          cost_refs: [String.t()],
          trace_refs: [String.t()],
          deduped?: boolean()
        }

  @enforce_keys [:batch_ref, :idempotency_key, :eval_ref, :score, :cost_refs, :trace_refs]
  defstruct [
    :batch_ref,
    :idempotency_key,
    :eval_ref,
    :score,
    :cost_refs,
    :trace_refs,
    deduped?: false
  ]
end

defmodule Mezzanine.OptimizationEngine.EvaluatorPool do
  @moduledoc """
  Deterministic distributed evaluator pool planning and result dedupe.
  """

  alias GEPAFramework.Value
  alias Mezzanine.OptimizationEngine.EvaluatorPool.{Batch, Lease, Result}
  alias Mezzanine.OptimizationEngine.RunSpec

  @type t :: %__MODULE__{run_ref: String.t(), batches: [Batch.t()], results_by_key: map()}

  @enforce_keys [:run_ref, :batches, :results_by_key]
  defstruct @enforce_keys

  @spec plan(RunSpec.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def plan(%RunSpec{} = spec, opts) when is_list(opts) do
    candidate_refs = Keyword.get(opts, :candidate_refs, [])
    example_refs = Keyword.get(opts, :example_refs, [])
    worker_ref = Keyword.get(opts, :worker_ref, "worker:unassigned")

    batches =
      candidate_refs
      |> Enum.with_index(1)
      |> Enum.map(fn {candidate_ref, index} ->
        batch(spec, candidate_ref, example_refs, worker_ref, index)
      end)

    {:ok, %__MODULE__{run_ref: spec.run_ref, batches: batches, results_by_key: %{}}}
  end

  @spec record_result(t(), map()) :: {:ok, t(), Result.t()} | {:error, term()}
  def record_result(%__MODULE__{} = pool, attrs) when is_map(attrs) do
    idempotency_key = Value.get(attrs, :idempotency_key)

    case Map.fetch(pool.results_by_key, idempotency_key) do
      {:ok, existing} ->
        {:ok, pool, %{existing | deduped?: true}}

      :error ->
        result = result(attrs)

        next_pool = %{
          pool
          | results_by_key: Map.put(pool.results_by_key, idempotency_key, result)
        }

        {:ok, next_pool, result}
    end
  end

  def record_result(%__MODULE__{}, _attrs), do: {:error, :invalid_evaluator_result}

  defp batch(spec, candidate_ref, example_refs, worker_ref, index) do
    batch_ref = "batch:" <> spec.run_ref <> ":" <> Integer.to_string(index)

    %Batch{
      batch_ref: batch_ref,
      run_ref: spec.run_ref,
      candidate_ref: candidate_ref,
      example_refs: example_refs,
      lease: %Lease{
        owner_ref: worker_ref,
        expires_at_ref: "lease_expiry:" <> spec.run_ref <> ":batch:" <> Integer.to_string(index)
      },
      idempotency_key:
        "idempotency:" <>
          spec.run_ref <> ":" <> candidate_ref <> ":batch:" <> Integer.to_string(index),
      retry_policy_ref: "retry:optimization:bounded",
      cost_refs: ["cost:optimization:batch:" <> Integer.to_string(index)],
      trace_refs: ["trace:optimization:batch:" <> Integer.to_string(index)]
    }
  end

  defp result(attrs) do
    %Result{
      batch_ref: Value.get(attrs, :batch_ref),
      idempotency_key: Value.get(attrs, :idempotency_key),
      eval_ref: Value.get(attrs, :eval_ref),
      score: Value.get(attrs, :score, 0.0),
      cost_refs: Value.string_list(Value.get(attrs, :cost_refs, [])),
      trace_refs: Value.string_list(Value.get(attrs, :trace_refs, []))
    }
  end
end
