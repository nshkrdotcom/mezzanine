defmodule Mezzanine.HeadlessCodingOps do
  @moduledoc """
  Headless coding-ops intake, readback, and operator command contracts.
  """

  defmodule WorkItem do
    @moduledoc """
    Ref-only headless coding work item accepted by the runtime boundary.
    """

    @type t :: %__MODULE__{
            tenant_ref: String.t(),
            request_ref: String.t(),
            session_ref: String.t(),
            provider_selection_ref: String.t(),
            target_selection_ref: String.t(),
            provider_account_ref: String.t(),
            connector_binding_ref: String.t(),
            credential_lease_ref: String.t(),
            target_ref: String.t(),
            operation_policy_ref: String.t(),
            idempotency_key: String.t(),
            trace_ref: String.t() | nil,
            receipt_ref: String.t(),
            status: :accepted
          }

    defstruct [
      :tenant_ref,
      :request_ref,
      :session_ref,
      :provider_selection_ref,
      :target_selection_ref,
      :provider_account_ref,
      :connector_binding_ref,
      :credential_lease_ref,
      :target_ref,
      :operation_policy_ref,
      :idempotency_key,
      :trace_ref,
      :receipt_ref,
      status: :accepted
    ]
  end

  defmodule ReadbackState do
    @moduledoc """
    Product-safe readback state for an admitted headless coding work item.
    """

    @type t :: %__MODULE__{
            work_item_ref: String.t() | nil,
            state:
              :completed
              | :failed
              | :stalled
              | :user_input_required
              | :rate_limited
              | :auth_required
              | :target_unavailable
              | :connector_blocked
              | :policy_denied,
            authority_refs: [String.t()],
            receipt_ref: String.t() | nil
          }

    defstruct [:work_item_ref, :state, :authority_refs, :receipt_ref]
  end

  defmodule OperatorCommand do
    @moduledoc """
    Ref-only operator command for an admitted headless coding work item.
    """

    @type t :: %__MODULE__{
            action:
              :cancel
              | :retry
              | :reassign_provider
              | :reassign_target
              | :request_human_input
              | :revoke_authority
              | :rotate_authority
              | :rotate_lease
              | :renew_authority
              | :rebind_authority
              | :detach_authority
              | :detach_target
              | :transfer_authority
              | :inspect_authority
              | :invalidate_authority
              | :resume_session,
            actor_ref: String.t() | nil,
            work_item_ref: String.t() | nil,
            authority_refs: [String.t()],
            idempotency_key: String.t() | nil
          }

    defstruct [:action, :actor_ref, :work_item_ref, :authority_refs, :idempotency_key]
  end

  @required_intake_refs [
    :tenant_ref,
    :request_ref,
    :session_ref,
    :provider_selection_ref,
    :target_selection_ref,
    :provider_account_ref,
    :connector_binding_ref,
    :credential_lease_ref,
    :target_ref,
    :operation_policy_ref,
    :idempotency_key
  ]

  @forbidden_material [
    :api_key,
    :authorization_header,
    :provider_payload,
    :raw_secret,
    :raw_token,
    :target_credentials,
    :token_file,
    :workspace_secret
  ]

  @states [
    :completed,
    :failed,
    :stalled,
    :user_input_required,
    :rate_limited,
    :auth_required,
    :target_unavailable,
    :connector_blocked,
    :policy_denied
  ]
  @state_lookup Map.new(@states, &{Atom.to_string(&1), &1})

  @actions [
    :cancel,
    :retry,
    :reassign_provider,
    :reassign_target,
    :request_human_input,
    :revoke_authority,
    :rotate_authority,
    :rotate_lease,
    :renew_authority,
    :rebind_authority,
    :detach_authority,
    :detach_target,
    :transfer_authority,
    :inspect_authority,
    :invalidate_authority,
    :resume_session
  ]
  @action_lookup Map.new(@actions, &{Atom.to_string(&1), &1})

  @known_fields @required_intake_refs ++
                  @forbidden_material ++
                  [
                    :trace_ref,
                    :work_item_ref,
                    :state,
                    :authority_refs,
                    :receipt_ref,
                    :action,
                    :actor_ref
                  ]

  @spec intake(map() | keyword()) ::
          {:ok, WorkItem.t()}
          | {:error, {:missing_headless_refs, [atom()]}}
          | {:error, {:forbidden_headless_material, [atom()]}}
  def intake(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize(attrs)

    case forbidden_material(attrs) do
      [] ->
        case missing_refs(attrs, @required_intake_refs) do
          [] -> {:ok, work_item(attrs)}
          missing -> {:error, {:missing_headless_refs, missing}}
        end

      forbidden ->
        {:error, {:forbidden_headless_material, forbidden}}
    end
  end

  @spec actions() :: [atom()]
  def actions, do: @actions

  @spec readback_state(map() | keyword()) ::
          {:ok, ReadbackState.t()} | {:error, {:invalid_headless_state, term()}}
  def readback_state(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize(attrs)

    case normalize_state(Map.get(attrs, :state)) do
      {:ok, state} ->
        {:ok,
         %ReadbackState{
           work_item_ref: Map.get(attrs, :work_item_ref),
           state: state,
           authority_refs: List.wrap(Map.get(attrs, :authority_refs, [])),
           receipt_ref: Map.get(attrs, :receipt_ref)
         }}

      :error ->
        {:error, {:invalid_headless_state, Map.get(attrs, :state)}}
    end
  end

  @spec operator_action(map() | keyword()) ::
          {:ok, OperatorCommand.t()} | {:error, {:invalid_operator_action, term()}}
  def operator_action(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize(attrs)

    case normalize_action(Map.get(attrs, :action)) do
      {:ok, action} ->
        {:ok,
         %OperatorCommand{
           action: action,
           actor_ref: Map.get(attrs, :actor_ref),
           work_item_ref: Map.get(attrs, :work_item_ref),
           authority_refs: List.wrap(Map.get(attrs, :authority_refs, [])),
           idempotency_key: Map.get(attrs, :idempotency_key)
         }}

      :error ->
        {:error, {:invalid_operator_action, Map.get(attrs, :action)}}
    end
  end

  defp work_item(attrs) do
    %WorkItem{
      tenant_ref: Map.fetch!(attrs, :tenant_ref),
      request_ref: Map.fetch!(attrs, :request_ref),
      session_ref: Map.fetch!(attrs, :session_ref),
      provider_selection_ref: Map.fetch!(attrs, :provider_selection_ref),
      target_selection_ref: Map.fetch!(attrs, :target_selection_ref),
      provider_account_ref: Map.fetch!(attrs, :provider_account_ref),
      connector_binding_ref: Map.fetch!(attrs, :connector_binding_ref),
      credential_lease_ref: Map.fetch!(attrs, :credential_lease_ref),
      target_ref: Map.fetch!(attrs, :target_ref),
      operation_policy_ref: Map.fetch!(attrs, :operation_policy_ref),
      idempotency_key: Map.fetch!(attrs, :idempotency_key),
      trace_ref: Map.get(attrs, :trace_ref),
      receipt_ref: "headless-coding-ops-receipt://tenant-1/#{Map.fetch!(attrs, :idempotency_key)}"
    }
  end

  defp forbidden_material(attrs), do: Enum.filter(@forbidden_material, &Map.has_key?(attrs, &1))
  defp missing_refs(attrs, fields), do: Enum.reject(fields, &present?(Map.get(attrs, &1)))

  defp normalize_state(value) when is_atom(value) do
    if value in @states, do: {:ok, value}, else: :error
  end

  defp normalize_state(value) when is_binary(value), do: Map.fetch(@state_lookup, value)
  defp normalize_state(_value), do: :error

  defp normalize_action(value) when is_atom(value) do
    if value in @actions, do: {:ok, value}, else: :error
  end

  defp normalize_action(value) when is_binary(value), do: Map.fetch(@action_lookup, value)
  defp normalize_action(_value), do: :error

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()

  defp normalize(attrs) when is_map(attrs) do
    Map.new(attrs, fn
      {key, value} when is_binary(key) -> {string_key(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp string_key(key), do: Enum.find(@known_fields, key, &(Atom.to_string(&1) == key))

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)
end
