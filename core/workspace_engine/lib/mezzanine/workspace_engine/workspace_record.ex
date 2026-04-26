defmodule Mezzanine.WorkspaceEngine.WorkspaceRecord do
  @moduledoc """
  Provider-neutral workspace allocation record.
  """

  @enforce_keys [
    :workspace_id,
    :installation_id,
    :subject_id,
    :logical_ref,
    :concrete_root,
    :concrete_path,
    :slug,
    :placement_kind,
    :cleanup_policy,
    :safety_hash
  ]
  defstruct [
    :workspace_id,
    :installation_id,
    :subject_id,
    :subject_ref,
    :logical_ref,
    :concrete_root,
    :concrete_path,
    :slug,
    :placement_kind,
    :cleanup_policy,
    :safety_hash,
    file_scope: %{},
    hook_specs: [],
    remote_hints: %{},
    status: :reserved,
    safety_status: :validated,
    reuse?: false,
    contract_version: "Mezzanine.WorkspaceRecord.v1"
  ]

  @type t :: %__MODULE__{
          contract_version: String.t(),
          workspace_id: String.t(),
          installation_id: String.t(),
          subject_id: String.t(),
          subject_ref: String.t() | nil,
          logical_ref: String.t(),
          concrete_root: String.t(),
          concrete_path: String.t(),
          slug: String.t(),
          placement_kind: atom(),
          cleanup_policy: atom(),
          safety_hash: String.t(),
          file_scope: map(),
          hook_specs: [map()],
          remote_hints: map(),
          status: atom(),
          safety_status: atom(),
          reuse?: boolean()
        }
end
