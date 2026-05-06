defmodule Mezzanine.ContextBudgetAdmission do
  @moduledoc """
  Fail-closed context-budget admission for governed AI effects.
  """

  alias OuterBrain.ContextBudget

  @loci [:preflight, :append, :stream, :runtime_admission, :reconciliation]

  defmodule Receipt do
    @moduledoc "Budget admission receipt."
    @enforce_keys [
      :locus,
      :tenant_ref,
      :authority_ref,
      :installation_ref,
      :idempotency_key,
      :trace_ref,
      :decision
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            locus: atom(),
            tenant_ref: String.t(),
            authority_ref: String.t(),
            installation_ref: String.t(),
            idempotency_key: String.t(),
            trace_ref: String.t(),
            decision: OuterBrain.MemoryContracts.ContextBudgetDecision.t()
          }
  end

  @spec loci() :: [atom()]
  def loci, do: @loci

  @spec preflight(ContextBudget.bucket(), non_neg_integer(), map()) ::
          {:ok, ContextBudget.bucket(), Receipt.t()} | {:error, term()}
  def preflight(bucket, requested_units, refs),
    do: admit(:preflight, bucket, requested_units, refs)

  @spec append(ContextBudget.bucket(), non_neg_integer(), map()) ::
          {:ok, ContextBudget.bucket(), Receipt.t()} | {:error, term()}
  def append(bucket, requested_units, refs), do: admit(:append, bucket, requested_units, refs)

  @spec stream(ContextBudget.bucket(), non_neg_integer(), map()) ::
          {:ok, ContextBudget.bucket(), Receipt.t()} | {:error, term()}
  def stream(bucket, requested_units, refs), do: admit(:stream, bucket, requested_units, refs)

  @spec runtime_admission(ContextBudget.bucket(), non_neg_integer(), map()) ::
          {:ok, ContextBudget.bucket(), Receipt.t()} | {:error, term()}
  def runtime_admission(bucket, requested_units, refs),
    do: admit(:runtime_admission, bucket, requested_units, refs)

  @spec reconciliation(ContextBudget.bucket(), non_neg_integer(), map()) ::
          {:ok, ContextBudget.bucket(), Receipt.t()} | {:error, term()}
  def reconciliation(bucket, requested_units, refs),
    do: admit(:reconciliation, bucket, requested_units, refs)

  @spec admit(atom(), ContextBudget.bucket(), non_neg_integer(), map()) ::
          {:ok, ContextBudget.bucket(), Receipt.t()} | {:error, term()}
  def admit(locus, %ContextBudget.Bucket{} = bucket, requested_units, refs)
      when locus in @loci and is_map(refs) do
    with :ok <- required_refs(refs),
         {:ok, updated_bucket, decision} <- ContextBudget.decide(bucket, requested_units) do
      if allowed?(decision) do
        {:ok, updated_bucket, receipt(locus, refs, decision)}
      else
        {:error, {:budget_denied, locus, decision}}
      end
    end
  end

  def admit(locus, _bucket, _requested_units, _refs) when locus in @loci,
    do: {:error, {:budget_denied, locus, :invalid_budget}}

  def admit(locus, _bucket, _requested_units, _refs), do: {:error, {:unknown_budget_locus, locus}}

  defp allowed?(%{decision: :allow}), do: true
  defp allowed?(%{decision: :allow_with_redaction}), do: true
  defp allowed?(_decision), do: false

  defp receipt(locus, refs, decision) do
    %Receipt{
      locus: locus,
      tenant_ref: fetch!(refs, :tenant_ref),
      authority_ref: fetch!(refs, :authority_ref),
      installation_ref: fetch!(refs, :installation_ref),
      idempotency_key: fetch!(refs, :idempotency_key),
      trace_ref: fetch!(refs, :trace_ref),
      decision: decision
    }
  end

  defp required_refs(refs) do
    required = [:tenant_ref, :authority_ref, :installation_ref, :idempotency_key, :trace_ref]

    case Enum.find(required, &(required_string(refs, &1) != :ok)) do
      nil -> :ok
      field -> {:error, {:missing_budget_admission_ref, field}}
    end
  end

  defp required_string(refs, field) do
    case Map.get(refs, field) || Map.get(refs, Atom.to_string(field)) do
      value when is_binary(value) ->
        if String.trim(value) == "", do: {:error, field}, else: :ok

      _other ->
        {:error, field}
    end
  end

  defp fetch!(refs, field), do: Map.get(refs, field) || Map.fetch!(refs, Atom.to_string(field))
end
