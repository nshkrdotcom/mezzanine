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

  defmodule HandoffResume do
    @moduledoc """
    Ref-only handoff and resume proof for a headless coding session.
    """

    @type t :: %__MODULE__{
            handoff_ref: String.t(),
            tenant_ref: String.t(),
            session_ref: String.t(),
            work_item_ref: String.t(),
            provider_account_ref: String.t(),
            connector_binding_ref: String.t(),
            credential_handle_ref: String.t(),
            credential_lease_ref: String.t(),
            native_auth_assertion_ref: String.t(),
            target_ref: String.t(),
            attach_grant_ref: String.t(),
            operation_policy_ref: String.t(),
            trace_ref: String.t(),
            idempotency_key: String.t(),
            active_execution_ref: String.t(),
            current_execution_ref: String.t(),
            restart_event:
              :target_detach
              | :sandbox_restart
              | :process_crash
              | :stream_reconnect
              | :workflow_resume,
            receipt_ref: String.t(),
            redacted?: true
          }

    defstruct [
      :handoff_ref,
      :tenant_ref,
      :session_ref,
      :work_item_ref,
      :provider_account_ref,
      :connector_binding_ref,
      :credential_handle_ref,
      :credential_lease_ref,
      :native_auth_assertion_ref,
      :target_ref,
      :attach_grant_ref,
      :operation_policy_ref,
      :trace_ref,
      :idempotency_key,
      :active_execution_ref,
      :current_execution_ref,
      :restart_event,
      :receipt_ref,
      redacted?: true
    ]
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
  @required_resume_refs [
    :handoff_ref,
    :tenant_ref,
    :session_ref,
    :work_item_ref,
    :provider_account_ref,
    :connector_binding_ref,
    :credential_handle_ref,
    :credential_lease_ref,
    :native_auth_assertion_ref,
    :target_ref,
    :attach_grant_ref,
    :operation_policy_ref,
    :trace_ref,
    :idempotency_key,
    :active_execution_ref,
    :current_execution_ref,
    :restart_event
  ]
  @restart_events [
    :target_detach,
    :sandbox_restart,
    :process_crash,
    :stream_reconnect,
    :workflow_resume
  ]
  @restart_event_lookup Map.new(@restart_events, &{Atom.to_string(&1), &1})

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
                    :actor_ref,
                    :handoff_ref,
                    :credential_handle_ref,
                    :native_auth_assertion_ref,
                    :attach_grant_ref,
                    :active_execution_ref,
                    :current_execution_ref,
                    :restart_event
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

  @spec resume_handoff(map() | keyword()) ::
          {:ok, HandoffResume.t()}
          | {:error, {:missing_headless_resume_refs, [atom()]}}
          | {:error, {:forbidden_headless_material, [atom()]}}
          | {:error, {:duplicate_active_execution_after_restart, map()}}
          | {:error, {:unsupported_restart_event, term()}}
  def resume_handoff(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize(attrs)

    with [] <- forbidden_material(attrs),
         [] <- missing_refs(attrs, @required_resume_refs),
         {:ok, restart_event} <- normalize_restart_event(Map.get(attrs, :restart_event)),
         :ok <- ensure_single_active_execution(attrs) do
      receipt_ref = "headless-coding-ops-resume://tenant-1/#{Map.fetch!(attrs, :idempotency_key)}"

      {:ok,
       %HandoffResume{
         handoff_ref: Map.fetch!(attrs, :handoff_ref),
         tenant_ref: Map.fetch!(attrs, :tenant_ref),
         session_ref: Map.fetch!(attrs, :session_ref),
         work_item_ref: Map.fetch!(attrs, :work_item_ref),
         provider_account_ref: Map.fetch!(attrs, :provider_account_ref),
         connector_binding_ref: Map.fetch!(attrs, :connector_binding_ref),
         credential_handle_ref: Map.fetch!(attrs, :credential_handle_ref),
         credential_lease_ref: Map.fetch!(attrs, :credential_lease_ref),
         native_auth_assertion_ref: Map.fetch!(attrs, :native_auth_assertion_ref),
         target_ref: Map.fetch!(attrs, :target_ref),
         attach_grant_ref: Map.fetch!(attrs, :attach_grant_ref),
         operation_policy_ref: Map.fetch!(attrs, :operation_policy_ref),
         trace_ref: Map.fetch!(attrs, :trace_ref),
         idempotency_key: Map.fetch!(attrs, :idempotency_key),
         active_execution_ref: Map.fetch!(attrs, :active_execution_ref),
         current_execution_ref: Map.fetch!(attrs, :current_execution_ref),
         restart_event: restart_event,
         receipt_ref: receipt_ref
       }}
    else
      forbidden when is_list(forbidden) and forbidden != [] ->
        {:error, {:forbidden_headless_material, forbidden}}

      missing when is_list(missing) and missing != [] ->
        {:error, {:missing_headless_resume_refs, missing}}

      {:error, {:unsupported_restart_event, event}} ->
        {:error, {:unsupported_restart_event, event}}

      {:error, {:duplicate_active_execution_after_restart, details}} ->
        {:error, {:duplicate_active_execution_after_restart, details}}
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

  defp normalize_restart_event(value) when is_atom(value) do
    if value in @restart_events,
      do: {:ok, value},
      else: {:error, {:unsupported_restart_event, value}}
  end

  defp normalize_restart_event(value) when is_binary(value) do
    case Map.fetch(@restart_event_lookup, value) do
      {:ok, event} -> {:ok, event}
      :error -> {:error, {:unsupported_restart_event, value}}
    end
  end

  defp normalize_restart_event(value), do: {:error, {:unsupported_restart_event, value}}

  defp ensure_single_active_execution(attrs) do
    active = Map.get(attrs, :active_execution_ref)
    current = Map.get(attrs, :current_execution_ref)

    if active == current do
      :ok
    else
      {:error,
       {:duplicate_active_execution_after_restart,
        %{active_execution_ref: active, current_execution_ref: current, redacted?: true}}}
    end
  end

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
