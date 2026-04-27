defmodule Mezzanine.IntegrationBridge.AuthorizedInvocation do
  @moduledoc """
  Post-Citadel lower invocation envelope.

  This struct is the physical boundary between Mezzanine governance and Jido
  Integration provider effects. Dispatchers must receive this struct, not a
  generic map, `RunIntent`, or `EffectIntent`.
  """

  @typedoc "Raw `Citadel.InvocationRequest.V2` struct or dumped map."
  @type invocation_request :: map() | struct()

  @type t :: %__MODULE__{
          tenant_id: String.t(),
          installation_id: String.t(),
          subject_id: String.t(),
          execution_id: String.t(),
          trace_id: String.t(),
          idempotency_key: String.t(),
          submission_dedupe_key: String.t(),
          invocation_request: invocation_request()
        }

  @enforce_keys [
    :tenant_id,
    :installation_id,
    :subject_id,
    :execution_id,
    :trace_id,
    :idempotency_key,
    :submission_dedupe_key,
    :invocation_request
  ]
  defstruct @enforce_keys

  @invocation_request_module :"Elixir.Citadel.InvocationRequest.V2"

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, Exception.t()}
  def new(attrs) do
    {:ok, new!(attrs)}
  rescue
    error in [ArgumentError, KeyError] -> {:error, error}
  end

  @spec new!(map() | keyword() | t()) :: t()
  def new!(%__MODULE__{} = invocation), do: validate!(invocation)

  def new!(attrs) when is_list(attrs) do
    attrs
    |> Enum.into(%{})
    |> new!()
  end

  def new!(attrs) when is_map(attrs) do
    invocation =
      %__MODULE__{
        tenant_id: required_string!(attrs, :tenant_id),
        installation_id: required_string!(attrs, :installation_id),
        subject_id: required_string!(attrs, :subject_id),
        execution_id: required_string!(attrs, :execution_id),
        trace_id: required_string!(attrs, :trace_id),
        idempotency_key: required_string!(attrs, :idempotency_key),
        submission_dedupe_key: required_string!(attrs, :submission_dedupe_key),
        invocation_request: required!(attrs, :invocation_request)
      }

    :ok = validate_expected_installation_revision!(attrs, invocation.invocation_request)

    validate!(invocation)
  end

  @spec default_capability!(t()) :: String.t()
  def default_capability!(%__MODULE__{} = invocation) do
    invocation.invocation_request
    |> normalized_request!()
    |> required!(:allowed_operations)
    |> case do
      [capability | _rest] when is_binary(capability) and capability != "" ->
        capability

      other ->
        raise ArgumentError,
              "AuthorizedInvocation.invocation_request.allowed_operations must include at least one capability, got: #{inspect(other)}"
    end
  end

  @spec authorize_capability!(t(), String.t()) :: :ok
  def authorize_capability!(%__MODULE__{} = invocation, capability_id)
      when is_binary(capability_id) and capability_id != "" do
    allowed_operations =
      invocation.invocation_request
      |> normalized_request!()
      |> required!(:allowed_operations)

    if capability_id in allowed_operations do
      :ok
    else
      raise ArgumentError,
            "AuthorizedInvocation capability #{inspect(capability_id)} is not present in Citadel authority allowed_operations"
    end
  end

  def authorize_capability!(%__MODULE__{}, capability_id) do
    raise ArgumentError,
          "AuthorizedInvocation capability must be a non-empty string, got: #{inspect(capability_id)}"
  end

  @spec invoke_input(t(), String.t()) :: map()
  def invoke_input(%__MODULE__{} = invocation, capability_id) do
    request = normalized_request!(invocation.invocation_request)
    authority_packet = required!(request, :authority_packet)
    execution_governance = required!(request, :execution_governance)

    %{
      tenant_id: invocation.tenant_id,
      installation_id: invocation.installation_id,
      subject_id: invocation.subject_id,
      execution_id: invocation.execution_id,
      trace_id: invocation.trace_id,
      idempotency_key: invocation.idempotency_key,
      submission_dedupe_key: invocation.submission_dedupe_key,
      capability_id: capability_id,
      invocation_request: invocation.invocation_request,
      authority: %{
        authority_packet_ref: authority_packet_ref(authority_packet),
        permission_decision_ref: required_string!(authority_packet, :decision_id),
        policy_version: required_string!(authority_packet, :policy_version),
        allowed_operations: required!(request, :allowed_operations),
        execution_governance_id: required_string!(execution_governance, :execution_governance_id)
      }
    }
  end

  defp validate!(%__MODULE__{} = invocation) do
    request = normalized_request!(invocation.invocation_request)
    authority_packet = required!(request, :authority_packet)
    execution_governance = required!(request, :execution_governance)
    execution_envelope = execution_envelope(request)

    require_equals!(required_string!(request, :tenant_id), invocation.tenant_id, :tenant_id)
    require_equals!(required_string!(request, :trace_id), invocation.trace_id, :trace_id)

    require_equals!(
      required_string!(execution_envelope, :installation_id),
      invocation.installation_id,
      :installation_id
    )

    require_equals!(required_string!(execution_envelope, :subject_id), invocation.subject_id, :subject_id)

    require_equals!(
      required_string!(execution_envelope, :execution_id),
      invocation.execution_id,
      :execution_id
    )

    require_equals!(
      required_string!(execution_envelope, :submission_dedupe_key),
      invocation.submission_dedupe_key,
      :submission_dedupe_key
    )

    _installation_revision = required!(execution_envelope, :installation_revision)

    :ok = validate_authority_packet!(authority_packet)
    :ok = validate_execution_governance!(execution_governance)
    :ok = authorize_capability!(invocation, default_capability!(invocation))

    invocation
  end

  defp validate_authority_packet!(packet) do
    _contract_version = require_equals!(required_string!(packet, :contract_version), "v1", :contract_version)
    _decision_id = required_string!(packet, :decision_id)
    _tenant_id = required_string!(packet, :tenant_id)
    _request_id = required_string!(packet, :request_id)
    _policy_version = required_string!(packet, :policy_version)
    :ok
  end

  defp validate_execution_governance!(packet) do
    _contract_version = require_equals!(required_string!(packet, :contract_version), "v1", :contract_version)
    _execution_governance_id = required_string!(packet, :execution_governance_id)
    _authority_ref = required!(packet, :authority_ref)
    _operations = required!(packet, :operations)
    :ok
  end

  defp validate_expected_installation_revision!(attrs, invocation_request) do
    case optional(attrs, :expected_installation_revision) do
      nil ->
        :ok

      expected_revision ->
        request = normalized_request!(invocation_request)
        actual_revision = request |> execution_envelope() |> required!(:installation_revision)

        case {expected_revision, actual_revision} do
          {expected, actual}
          when is_integer(expected) and expected >= 0 and is_integer(actual) and actual >= 0 and
                 expected == actual ->
            :ok

          {expected, actual}
          when is_integer(expected) and expected >= 0 and is_integer(actual) and actual >= 0 ->
            raise ArgumentError,
                  "AuthorizedInvocation stale installation_revision: expected #{inspect(expected)}, got #{inspect(actual)}"

          {expected, _actual} ->
            raise ArgumentError,
                  "AuthorizedInvocation expected_installation_revision must be a non-negative integer, got: #{inspect(expected)}"
        end
    end
  end

  defp normalized_request!(%{__struct__: @invocation_request_module} = request), do: Map.from_struct(request)

  defp normalized_request!(%{} = request) do
    if Map.has_key?(request, :schema_version) or Map.has_key?(request, "schema_version") do
      require_equals!(required!(request, :schema_version), 2, :schema_version)
      request
    else
      raise ArgumentError,
            "AuthorizedInvocation.invocation_request must be a Citadel.InvocationRequest.V2 struct or map representation"
    end
  end

  defp normalized_request!(request) do
    raise ArgumentError,
          "AuthorizedInvocation.invocation_request must be a Citadel.InvocationRequest.V2 struct or map representation, got: #{inspect(request)}"
  end

  defp execution_envelope(request) do
    request
    |> required!(:extensions)
    |> required!("citadel")
    |> required!("execution_envelope")
  end

  defp authority_packet_ref(authority_packet) do
    "authority-decision://#{required_string!(authority_packet, :decision_id)}"
  end

  defp require_equals!(actual, expected, _field) when actual == expected, do: actual

  defp require_equals!(actual, expected, field) do
    raise ArgumentError,
          "AuthorizedInvocation #{field} mismatch: expected #{inspect(expected)}, got #{inspect(actual)}"
  end

  defp required_string!(attrs, key) do
    case required!(attrs, key) do
      value when is_binary(value) ->
        if String.trim(value) == "" do
          raise ArgumentError, "AuthorizedInvocation #{key} must be a non-empty string"
        end

        value

      value ->
        raise ArgumentError,
              "AuthorizedInvocation #{key} must be a non-empty string, got: #{inspect(value)}"
    end
  end

  defp required!(attrs, key) when is_map(attrs) do
    cond do
      Map.has_key?(attrs, key) ->
        Map.fetch!(attrs, key)

      is_atom(key) and Map.has_key?(attrs, Atom.to_string(key)) ->
        Map.fetch!(attrs, Atom.to_string(key))

      true ->
        raise ArgumentError, "AuthorizedInvocation missing required field #{inspect(key)}"
    end
  end

  defp required!(attrs, key) do
    raise ArgumentError,
          "AuthorizedInvocation expected a map while reading #{inspect(key)}, got: #{inspect(attrs)}"
  end

  defp optional(attrs, key) when is_map(attrs) do
    cond do
      Map.has_key?(attrs, key) ->
        Map.fetch!(attrs, key)

      is_atom(key) and Map.has_key?(attrs, Atom.to_string(key)) ->
        Map.fetch!(attrs, Atom.to_string(key))

      true ->
        nil
    end
  end
end
