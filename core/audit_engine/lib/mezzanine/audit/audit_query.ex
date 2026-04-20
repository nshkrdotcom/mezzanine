defmodule Mezzanine.Audit.AuditQuery do
  @moduledoc """
  Audit-owned read helpers for callers that need audit facts as classifier
  evidence without importing the audit aggregate directly.
  """

  alias Mezzanine.Audit.AuditFact
  require Ash.Query

  @spec decision_terminal_resolution_attempts(String.t(), String.t()) ::
          {:ok, [AuditFact.t()]} | {:error, term()}
  def decision_terminal_resolution_attempts(installation_id, decision_id)
      when is_binary(installation_id) and is_binary(decision_id) do
    AuditFact
    |> Ash.Query.filter(
      installation_id == ^installation_id and decision_id == ^decision_id and
        fact_kind == :decision_terminal_resolution_attempt
    )
    |> Ash.read(authorize?: false, domain: Mezzanine.Audit)
  end
end
