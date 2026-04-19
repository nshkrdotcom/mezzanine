defmodule Mezzanine.ControlRoom.SuppressionVisibility do
  @moduledoc """
  Operator-visible suppression and quarantine contract.

  Contract: `Platform.SuppressionVisibility.v1`.
  """

  alias Mezzanine.ControlRoom.ResourcePressureSupport

  @contract_name "Platform.SuppressionVisibility.v1"
  @required_binary_fields ResourcePressureSupport.base_required_binary_fields() ++
                            [
                              :suppression_ref,
                              :suppression_kind,
                              :reason_code,
                              :target_ref,
                              :operator_visibility,
                              :diagnostics_ref
                            ]
  @optional_binary_fields ResourcePressureSupport.optional_actor_fields()

  defstruct [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :suppression_ref,
    :suppression_kind,
    :reason_code,
    :target_ref,
    :operator_visibility,
    :recovery_action_refs,
    :diagnostics_ref
  ]

  @type t :: %__MODULE__{}

  @spec contract_name() :: String.t()
  def contract_name, do: @contract_name

  @spec new(map() | keyword()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, :invalid_suppression_visibility}
  def new(attrs) do
    with {:ok, attrs} <- ResourcePressureSupport.normalize_attrs(attrs),
         [] <- missing_required_fields(attrs),
         true <- ResourcePressureSupport.optional_binary_fields?(attrs, @optional_binary_fields),
         true <- Map.fetch!(attrs, :operator_visibility) == "visible" do
      {:ok, build(attrs)}
    else
      fields when is_list(fields) -> {:error, {:missing_required_fields, fields}}
      _error -> {:error, :invalid_suppression_visibility}
    end
  end

  defp build(attrs) do
    actors = ResourcePressureSupport.actor_fields(attrs)

    struct!(
      __MODULE__,
      Map.merge(attrs, %{
        contract_name: @contract_name,
        principal_ref: actors.principal_ref,
        system_actor_ref: actors.system_actor_ref
      })
    )
  end

  defp missing_required_fields(attrs) do
    ResourcePressureSupport.missing_required_fields(attrs, @required_binary_fields, [], []) ++
      recovery_action_missing(attrs)
  end

  defp recovery_action_missing(attrs) do
    case Map.get(attrs, :recovery_action_refs) do
      refs when is_list(refs) ->
        if refs != [] and Enum.all?(refs, &ResourcePressureSupport.present_binary?/1) do
          []
        else
          [:recovery_action_refs]
        end

      _other ->
        [:recovery_action_refs]
    end
  end
end
