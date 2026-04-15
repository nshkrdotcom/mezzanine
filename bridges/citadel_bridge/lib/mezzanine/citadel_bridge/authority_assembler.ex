defmodule Mezzanine.CitadelBridge.AuthorityAssembler do
  @moduledoc """
  Assembles the public Citadel host-ingress request context from a Mezzanine
  run intent plus caller-supplied routing metadata.
  """

  alias Citadel.HostIngress.RequestContext
  alias MezzanineOpsModel.Intent.RunIntent

  @spec request_context(RunIntent.t(), map()) :: {:ok, RequestContext.t()} | {:error, term()}
  def request_context(%RunIntent{} = intent, attrs \\ %{}) when is_map(attrs) do
    metadata = Map.new(intent.metadata)

    {:ok,
     RequestContext.new!(%{
       request_id: value(attrs, :request_id, intent.intent_id),
       session_id: value(attrs, :session_id, "work/#{intent.work_id}"),
       tenant_id: value(attrs, :tenant_id, value(metadata, :tenant_id, "tenant/unknown")),
       actor_id: value(attrs, :actor_id, value(metadata, :actor_id, "mezzanine")),
       trace_id: value(attrs, :trace_id, intent.intent_id),
       trace_origin: value(attrs, :trace_origin, "mezzanine.citadel_bridge"),
       idempotency_key: value(attrs, :idempotency_key, intent.intent_id),
       host_request_id: value(attrs, :host_request_id, intent.intent_id),
       environment: value(attrs, :environment, value(metadata, :environment, "dev")),
       policy_epoch: Map.get(attrs, :policy_epoch, 0),
       metadata_keys: metadata_keys(metadata)
     })}
  rescue
    error in ArgumentError -> {:error, {:invalid_request_context, Exception.message(error)}}
  end

  defp metadata_keys(metadata) do
    metadata
    |> Map.keys()
    |> Enum.map(&to_string/1)
    |> Enum.sort()
  end

  defp value(map, key, default) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key)) || default
  end
end
