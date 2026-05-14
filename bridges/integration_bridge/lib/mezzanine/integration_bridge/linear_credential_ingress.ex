defmodule Mezzanine.IntegrationBridge.LinearCredentialIngress do
  @moduledoc """
  Linear command-credential ingress for product live checks.

  The public result contains only a Jido connection id, credential ref metadata,
  and an `AuthorizedInvocation`. Raw credential material is installed into Jido
  auth and is intentionally not returned.
  """

  alias Jido.Integration.V2
  alias Jido.Integration.V2.Connectors.Linear
  alias Jido.Integration.V2.Connectors.Linear.InstallBinding
  alias Mezzanine.IntegrationBridge.AuthorizedInvocation

  @default_allowed_operations [
    "linear.users.get_self",
    "linear.issues.list",
    "linear.comments.create",
    "linear.comments.update",
    "linear.issues.update",
    "linear.workflow_states.list"
  ]

  @spec prepare_api_key_invocation(String.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def prepare_api_key_invocation(api_key, attrs, opts \\ [])

  def prepare_api_key_invocation(api_key, attrs, opts)
      when is_binary(api_key) and (is_map(attrs) or is_list(attrs)) and is_list(opts) do
    attrs = normalize_attrs(attrs)

    with :ok <- V2.register_connector(Linear),
         {:ok, binding} <- api_key_binding(api_key),
         {:ok, install_result} <- install_connection(binding, attrs, opts),
         {:ok, invocation} <- AuthorizedInvocation.new(invocation_attrs(attrs)) do
      connection = Map.fetch!(install_result, :connection)
      credential_ref = Map.fetch!(install_result, :credential_ref)

      {:ok,
       %{
         authorized_invocation: invocation,
         connection_id: connection.connection_id,
         credential_ref_id: credential_ref.id,
         source_opts: [
           invoke_opts: [
             connection_id: connection.connection_id,
             actor_id: value(attrs, :actor_id)
           ],
           credential_ref_id: credential_ref.id,
           credential_redeemed?: true
         ]
       }}
    end
  end

  def prepare_api_key_invocation(_api_key, _attrs, _opts), do: {:error, :invalid_linear_api_key}

  @spec prepare_connection_invocation(String.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def prepare_connection_invocation(connection_id, attrs, opts \\ [])

  def prepare_connection_invocation(connection_id, attrs, opts)
      when is_binary(connection_id) and connection_id != "" and
             (is_map(attrs) or is_list(attrs)) and is_list(opts) do
    attrs = normalize_attrs(attrs)

    with {:ok, invocation} <- AuthorizedInvocation.new(invocation_attrs(attrs)) do
      credential_ref_id =
        string_value(opts, :credential_ref_id) || string_value(opts, :credential_ref)

      {:ok,
       %{
         authorized_invocation: invocation,
         connection_id: connection_id,
         credential_ref_id: credential_ref_id,
         source_opts:
           [
             invoke_opts: [
               connection_id: connection_id,
               actor_id: value(attrs, :actor_id)
             ],
             credential_redeemed?: true
           ]
           |> maybe_put(:credential_ref_id, credential_ref_id)
           |> maybe_put(:credential_lease_ref, string_value(opts, :credential_lease_ref))
       }}
    end
  end

  def prepare_connection_invocation(_connection_id, _attrs, _opts),
    do: {:error, :invalid_linear_connection_id}

  defp api_key_binding(api_key) do
    {:ok, InstallBinding.from_api_key(api_key)}
  rescue
    error in [ArgumentError] -> {:error, error}
  end

  defp install_connection(binding, attrs, opts) do
    now = Keyword.get(opts, :now) || DateTime.utc_now()
    auth = Linear.manifest().auth
    tenant_id = required_string!(attrs, :tenant_id)

    subject =
      string_value(attrs, :subject) || string_value(attrs, :actor_id) || "linear-live-proof"

    actor_id = string_value(attrs, :actor_id) || subject
    requested_scopes = Map.get(auth, :requested_scopes, [])

    with {:ok, %{install: install}} <-
           V2.start_install("linear", tenant_id, %{
             actor_id: actor_id,
             auth_type: auth.auth_type,
             profile_id: auth.default_profile,
             subject: subject,
             requested_scopes: requested_scopes,
             now: now
           }) do
      V2.complete_install(
        install.install_id,
        InstallBinding.complete_install_attrs(subject, requested_scopes, binding, now: now)
      )
    end
  end

  defp invocation_attrs(attrs) do
    tenant_id = required_string!(attrs, :tenant_id)
    installation_id = required_string!(attrs, :installation_id)
    subject_id = required_string!(attrs, :subject_id)
    execution_id = required_string!(attrs, :execution_id)
    trace_id = required_string!(attrs, :trace_id)
    idempotency_key = required_string!(attrs, :idempotency_key)
    submission_dedupe_key = required_string!(attrs, :submission_dedupe_key)
    allowed_operations = allowed_operations(attrs)
    decision_id = "decision-#{execution_id}"

    %{
      tenant_id: tenant_id,
      installation_id: installation_id,
      subject_id: subject_id,
      execution_id: execution_id,
      trace_id: trace_id,
      idempotency_key: idempotency_key,
      submission_dedupe_key: submission_dedupe_key,
      invocation_request: %{
        schema_version: 2,
        invocation_request_id: "invoke-#{execution_id}",
        request_id: "request-#{execution_id}",
        session_id: "session-#{execution_id}",
        tenant_id: tenant_id,
        trace_id: trace_id,
        actor_id: string_value(attrs, :actor_id) || "operator",
        target_id: string_value(attrs, :target_id) || subject_id,
        target_kind: "runtime_target",
        selected_step_id: string_value(attrs, :selected_step_id) || "linear-live",
        allowed_operations: allowed_operations,
        authority_packet: %{
          contract_version: "v1",
          decision_id: decision_id,
          tenant_id: tenant_id,
          request_id: "request-#{execution_id}",
          policy_version: string_value(attrs, :policy_version) || "live-product-command-v1",
          boundary_class: "workspace_session",
          trust_profile: "baseline",
          approval_profile: "standard",
          egress_profile: "restricted",
          workspace_profile: "workspace",
          resource_profile: "standard",
          decision_hash: String.duplicate("a", 64),
          extensions: %{"citadel" => %{}}
        },
        boundary_intent: %{},
        topology_intent: %{},
        execution_governance: %{
          "contract_version" => "v1",
          "execution_governance_id" => "governance-#{execution_id}",
          "authority_ref" => %{"decision_id" => decision_id},
          "operations" => %{"allowed_operations" => allowed_operations},
          "sandbox" => %{},
          "credentials" => %{},
          "resources" => %{}
        },
        extensions: %{
          "citadel" => %{
            "execution_envelope" => %{
              "installation_id" => installation_id,
              "installation_revision" => 1,
              "subject_id" => subject_id,
              "execution_id" => execution_id,
              "submission_dedupe_key" => submission_dedupe_key
            }
          }
        }
      }
    }
  end

  defp allowed_operations(attrs) do
    attrs
    |> value(:allowed_operations)
    |> case do
      values when is_list(values) -> values
      _other -> @default_allowed_operations
    end
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> case do
      [] -> @default_allowed_operations
      values -> values
    end
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(%{} = attrs), do: Map.new(attrs)

  defp required_string!(attrs, key) do
    case string_value(attrs, key) do
      value when is_binary(value) and value != "" -> value
      _missing -> raise ArgumentError, "#{key} is required"
    end
  end

  defp string_value(attrs, key) do
    case value(attrs, key) do
      value when is_binary(value) -> value
      value when is_atom(value) -> Atom.to_string(value)
      _other -> nil
    end
  end

  defp value(list, key) when is_list(list), do: Keyword.get(list, key)
  defp value(%{} = map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, _key, ""), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end
