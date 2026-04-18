defmodule Mezzanine.LowerGateway do
  @moduledoc """
  Execution-facing lower-gateway seam for durable dispatch and dedupe lookup.
  """

  @type dispatch_claim :: %{
          execution_id: Ecto.UUID.t(),
          tenant_id: String.t(),
          installation_id: String.t(),
          subject_id: Ecto.UUID.t(),
          trace_id: String.t(),
          causation_id: String.t() | nil,
          submission_dedupe_key: String.t(),
          compiled_pack_revision: integer(),
          binding_snapshot: map(),
          dispatch_envelope: map()
        }

  @type dispatch_result ::
          {:accepted, map()}
          | {:rejected, map()}
          | {:semantic_failure, map()}
          | {:error, {:retryable, term(), map()}}
          | {:error, {:terminal, term(), map()}}
          | {:error, {:semantic_failure, map()}}
          | {:error, term()}

  @type lookup_result ::
          :never_seen
          | {:rejected, map()}
          | {:accepted, %{submission_ref: map(), lower_receipt: map()}}
          | {:expired, DateTime.t()}
          | {:error, term()}

  @type execution_lookup :: %{
          optional(:submission_ref) => map(),
          optional(:submission_dedupe_key) => String.t(),
          optional(:lower_receipt) => map()
        }

  @type execution_outcome :: %{
          receipt_id: String.t(),
          status: :ok | :error | :cancelled,
          lower_receipt: map(),
          normalized_outcome: map(),
          lifecycle_hints: map(),
          failure_kind: atom() | nil,
          artifact_refs: [String.t()],
          observed_at: DateTime.t()
        }

  @type outcome_result :: :pending | {:ok, execution_outcome()} | {:error, term()}
  @type cancel_result ::
          {:cancelled, DateTime.t()}
          | {:too_late, execution_outcome() | map()}
          | {:error, term()}

  @callback dispatch(dispatch_claim()) :: dispatch_result()
  @callback lookup_submission(String.t(), String.t()) :: lookup_result()
  @callback fetch_execution_outcome(execution_lookup(), String.t()) :: outcome_result()
  @callback request_cancel(map(), String.t(), map()) :: cancel_result()

  @spec dispatch(dispatch_claim()) :: dispatch_result()
  def dispatch(claim), do: implementation().dispatch(claim)

  @spec lookup_submission(String.t(), String.t()) :: lookup_result()
  def lookup_submission(submission_dedupe_key, tenant_id) do
    implementation().lookup_submission(submission_dedupe_key, tenant_id)
  end

  @spec fetch_execution_outcome(execution_lookup(), String.t()) :: outcome_result()
  def fetch_execution_outcome(execution_lookup, tenant_id)
      when is_map(execution_lookup) and is_binary(tenant_id) do
    implementation().fetch_execution_outcome(execution_lookup, tenant_id)
  end

  @spec request_cancel(map(), String.t(), map()) :: cancel_result()
  def request_cancel(submission_ref, tenant_id, reason) do
    implementation().request_cancel(submission_ref, tenant_id, reason)
  end

  defp implementation do
    Application.get_env(
      :mezzanine_execution_engine,
      :lower_gateway_impl,
      Mezzanine.LowerGateway.Unconfigured
    )
  end
end

defmodule Mezzanine.LowerGateway.Unconfigured do
  @moduledoc false

  @behaviour Mezzanine.LowerGateway

  @impl true
  def dispatch(_claim), do: {:error, {:lower_gateway_unconfigured, __MODULE__}}

  @impl true
  def lookup_submission(_submission_dedupe_key, _tenant_id),
    do: {:error, {:lower_gateway_unconfigured, __MODULE__}}

  @impl true
  def fetch_execution_outcome(_execution_lookup, _tenant_id),
    do: {:error, {:lower_gateway_unconfigured, __MODULE__}}

  @impl true
  def request_cancel(_submission_ref, _tenant_id, _reason),
    do: {:error, {:lower_gateway_unconfigured, __MODULE__}}
end
