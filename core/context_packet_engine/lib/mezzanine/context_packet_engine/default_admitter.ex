defmodule Mezzanine.ContextPacketEngine.DefaultAdmitter do
  @moduledoc """
  Pure, deterministic MVP packet admission implementation.
  """

  @behaviour Mezzanine.ContextPacketEngine.Admitter

  alias Citadel.ContextAuthority.Grant
  alias Mezzanine.ContextPacketEngine.{AdmissionReceipt, AdmissionRequest}
  alias OuterBrain.ContextABI.{Canonical, ContextPacket, Failure}

  @impl true
  def admit(%ContextPacket{} = packet, request_attrs, opts) when is_list(opts) do
    with {:ok, request} <- AdmissionRequest.new(request_attrs),
         :none <- duplicate_receipt(request, opts),
         :ok <- valid_packet?(packet, request),
         :ok <- authority_allowed?(packet, request, opts),
         :ok <- budget_allowed?(request, opts) do
      {:ok, AdmissionReceipt.admitted(packet, request)}
    else
      {:duplicate, %AdmissionReceipt{} = receipt} ->
        {:ok, AdmissionReceipt.duplicate(receipt)}

      {:error, %Failure{} = failure} ->
        {:error, failure}
    end
  end

  def admit(_packet, request_attrs, _opts) do
    trace_ref = trace_ref(request_attrs)

    failure("mezzanine.packet_admission.invalid_packet.v1",
      safe_message: "context packet is invalid",
      trace_ref: trace_ref
    )
  end

  defp duplicate_receipt(%AdmissionRequest{} = request, opts) do
    receipts = Keyword.get(opts, :admitted_receipts, %{})

    case Map.get(receipts, request.idempotency_key) ||
           Map.get(receipts, to_string(request.idempotency_key)) do
      %AdmissionReceipt{} = receipt -> {:duplicate, receipt}
      _other -> :none
    end
  end

  defp valid_packet?(%ContextPacket{} = packet, %AdmissionRequest{} = request) do
    cond do
      packet.schema_ref != ContextPacket.schema_ref() ->
        failure("mezzanine.packet_admission.stale_packet_schema.v1",
          safe_message: "context packet schema is not supported",
          trace_ref: request.trace_ref,
          evidence_refs: ["schema://#{packet.schema_ref}"]
        )

      packet.context_packet_ref != request.context_packet_ref ->
        failure("mezzanine.packet_admission.packet_ref_mismatch.v1",
          safe_message: "context packet ref does not match admission request",
          trace_ref: request.trace_ref
        )

      packet.tenant_ref != request.tenant_ref ->
        failure("mezzanine.packet_admission.tenant_mismatch.v1",
          safe_message: "context packet tenant does not match admission request",
          trace_ref: request.trace_ref
        )

      packet.packet_hash != Canonical.packet_hash(packet) ->
        failure("mezzanine.packet_admission.stale_packet_hash.v1",
          safe_message: "context packet hash is stale",
          trace_ref: request.trace_ref,
          evidence_refs: [packet.context_packet_ref]
        )

      true ->
        :ok
    end
  end

  defp authority_allowed?(%ContextPacket{} = packet, %AdmissionRequest{} = request, opts) do
    with {:ok, grant} <- authority_grant(opts, request),
         :ok <- grant_not_expired(grant, opts),
         :ok <- grant_matches_packet(grant, packet, request) do
      grant_allows_model_classes(grant, packet, request)
    end
  end

  defp authority_grant(opts, request) do
    case Keyword.get(opts, :authority_grant) do
      nil ->
        failure("mezzanine.packet_admission.authority_required.v1",
          safe_message: "packet admission requires Citadel authority",
          trace_ref: request.trace_ref,
          retryable?: true,
          evidence_refs: [request.authority_ref]
        )

      %Grant{} = grant ->
        {:ok, grant}

      attrs when is_map(attrs) or is_list(attrs) ->
        case Grant.new(attrs) do
          {:ok, grant} ->
            {:ok, grant}

          {:error, _reason} ->
            failure("mezzanine.packet_admission.invalid_authority_grant.v1",
              safe_message: "packet admission authority grant is invalid",
              trace_ref: request.trace_ref,
              evidence_refs: [request.authority_ref]
            )
        end
    end
  end

  defp grant_not_expired(%Grant{} = grant, opts) do
    now = Keyword.get_lazy(opts, :now, fn -> DateTime.utc_now() end)

    if Grant.expired?(grant, now) do
      failure("mezzanine.packet_admission.authority_expired.v1",
        safe_message: "packet admission authority grant has expired",
        trace_ref: grant.trace_ref,
        evidence_refs: [grant.authority_ref]
      )
    else
      :ok
    end
  end

  defp grant_matches_packet(
         %Grant{} = grant,
         %ContextPacket{} = packet,
         %AdmissionRequest{} = request
       ) do
    cond do
      grant.authority_ref != request.authority_ref ->
        failure("mezzanine.packet_admission.authority_ref_mismatch.v1",
          safe_message: "authority grant ref does not match admission request",
          trace_ref: request.trace_ref
        )

      grant.tenant_ref != packet.tenant_ref ->
        failure("mezzanine.packet_admission.authority_tenant_mismatch.v1",
          safe_message: "authority grant tenant does not match packet tenant",
          trace_ref: request.trace_ref
        )

      grant.route_policy_ref != packet.route_policy_ref ->
        failure("mezzanine.packet_admission.route_policy_mismatch.v1",
          safe_message: "authority grant route policy does not match packet",
          trace_ref: request.trace_ref
        )

      true ->
        :ok
    end
  end

  defp grant_allows_model_classes(%Grant{} = grant, %ContextPacket{} = packet, request) do
    denied = packet.model_class_allowlist -- grant.allowed_model_classes

    if denied == [] do
      :ok
    else
      failure("mezzanine.packet_admission.model_class_denied.v1",
        safe_message: "authority grant does not allow requested model classes",
        trace_ref: request.trace_ref,
        evidence_refs: denied
      )
    end
  end

  defp budget_allowed?(%AdmissionRequest{} = request, opts) do
    case Keyword.get(opts, :budget_decision, :allow) do
      decision when decision in [:allow, :allow_warn_soft, :allow_with_override] ->
        :ok

      %{decision: decision} when decision in [:allow, :allow_with_redaction] ->
        :ok

      %{decision_class: decision}
      when decision in [:allow, :allow_warn_soft, :allow_with_override] ->
        :ok

      other ->
        failure("mezzanine.packet_admission.budget_exhausted.v1",
          safe_message: "packet admission budget gate denied execution",
          trace_ref: request.trace_ref,
          evidence_refs: ["budget-decision://#{inspect(other)}"]
        )
    end
  end

  defp trace_ref(%{trace_ref: trace_ref}) when is_binary(trace_ref), do: trace_ref
  defp trace_ref(%{"trace_ref" => trace_ref}) when is_binary(trace_ref), do: trace_ref
  defp trace_ref(_attrs), do: nil

  defp failure(reason_code, opts) do
    {:ok, failure} =
      Failure.new(%{
        owner: :mezzanine,
        reason_code: reason_code,
        safe_message: Keyword.fetch!(opts, :safe_message),
        retryable?: Keyword.get(opts, :retryable?, false),
        trace_ref: Keyword.get(opts, :trace_ref),
        evidence_refs: Keyword.get(opts, :evidence_refs, [])
      })

    {:error, failure}
  end
end
