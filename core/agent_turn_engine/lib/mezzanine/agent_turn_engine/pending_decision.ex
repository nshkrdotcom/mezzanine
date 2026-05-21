defmodule Mezzanine.AgentTurnEngine.PendingDecision do
  @moduledoc """
  Decision binding used to resume an open pending interaction.

  The decision carries the authority revision that approved the resume. The
  store layer compares this against the ledger authority before it allows the
  pending interaction to resolve.
  """

  alias Mezzanine.AgentTurnEngine.Validation

  @decisions [:approved, :denied, :expired, :cancelled]

  @enforce_keys [
    :decision_ref,
    :pending_ref,
    :tenant_ref,
    :actor_ref,
    :authority_ref,
    :authority_revision_ref,
    :decision,
    :idempotency_key,
    :decided_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{}

  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    with :ok <- Validation.reject_lower_leakage(attrs),
         :ok <- Validation.ref(attrs, :decision_ref, "decision://"),
         :ok <- Validation.ref(attrs, :pending_ref, "agent-pending://"),
         :ok <- Validation.ref(attrs, :tenant_ref, "tenant://"),
         :ok <- Validation.ref(attrs, :actor_ref, "actor://"),
         :ok <- Validation.ref(attrs, :authority_ref, "authority://"),
         :ok <- Validation.ref(attrs, :authority_revision_ref, "authority-revision://"),
         :ok <- Validation.one_of(attrs, :decision, @decisions),
         :ok <- Validation.required_binary(attrs, :idempotency_key),
         :ok <- Validation.datetime(attrs, :decided_at) do
      {:ok, struct!(__MODULE__, Validation.take(attrs, __struct__()))}
    end
  end

  @spec new!(map()) :: t()
  def new!(attrs), do: Validation.new!(__MODULE__, attrs, &new/1)
end
