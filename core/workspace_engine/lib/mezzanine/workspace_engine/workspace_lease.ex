defmodule Mezzanine.WorkspaceEngine.WorkspaceLease do
  @moduledoc """
  Execution-scoped lease for a workspace record.
  """

  @enforce_keys [:lease_id, :workspace_id, :execution_id, :lease_owner]
  defstruct [
    :lease_id,
    :workspace_id,
    :execution_id,
    :lease_owner,
    :expires_at,
    :release_reason,
    lease_state: :active,
    contract_version: "Mezzanine.WorkspaceLease.v1"
  ]

  @type t :: %__MODULE__{
          contract_version: String.t(),
          lease_id: String.t(),
          workspace_id: String.t(),
          execution_id: String.t(),
          lease_owner: String.t(),
          lease_state: atom(),
          expires_at: DateTime.t() | nil,
          release_reason: String.t() | nil
        }
end
