defmodule Mezzanine.ConfigRegistry.ClusterInvalidation do
  @moduledoc """
  Minimum cluster invalidation message contract for policy/cache fanout.
  """

  @topic_segment_regex ~r/\A[a-z0-9_-]+\z/
  @topic_regex ~r/\A[a-z0-9_-]+(\.[a-z0-9_-]+)*\z/
  @global_tenant_ref "tenant://global"
  @global_installation_ref "installation://global"
  @cache_fanout_topic "memory.cache_invalidation"
  @telemetry_event [:mezzanine, :cluster_invalidation, :publish]

  @enforce_keys [
    :invalidation_id,
    :tenant_ref,
    :topic,
    :source_node_ref,
    :commit_lsn,
    :commit_hlc,
    :published_at
  ]
  defstruct [
    :invalidation_id,
    :tenant_ref,
    :topic,
    :source_node_ref,
    :commit_lsn,
    :commit_hlc,
    :published_at,
    metadata: %{}
  ]

  @commit_hlc_key_lookup %{"l" => :l, "n" => :n, "w" => :w}

  @type t :: %__MODULE__{
          invalidation_id: String.t(),
          tenant_ref: String.t(),
          topic: String.t(),
          source_node_ref: String.t(),
          commit_lsn: String.t(),
          commit_hlc: map(),
          published_at: DateTime.t(),
          metadata: map()
        }

  @spec new(t() | map() | keyword()) :: {:ok, t()} | {:error, Exception.t()}
  def new(%__MODULE__{} = message), do: new(Map.from_struct(message))

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    {:ok, new!(attrs)}
  rescue
    error in ArgumentError -> {:error, error}
  end

  @spec new!(t() | map() | keyword()) :: t()
  def new!(%__MODULE__{} = message), do: new!(Map.from_struct(message))

  def new!(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    %__MODULE__{
      invalidation_id: attrs |> fetch(:invalidation_id) |> required_string(:invalidation_id),
      tenant_ref: attrs |> fetch(:tenant_ref) |> required_string(:tenant_ref),
      topic: topic!(fetch(attrs, :topic), :topic),
      source_node_ref: attrs |> fetch(:source_node_ref) |> required_string(:source_node_ref),
      commit_lsn: attrs |> fetch(:commit_lsn) |> required_string(:commit_lsn),
      commit_hlc: commit_hlc!(fetch(attrs, :commit_hlc)),
      published_at: datetime!(fetch(attrs, :published_at), :published_at),
      metadata: metadata!(fetch(attrs, :metadata))
    }
  end

  @spec dump(t()) :: map()
  def dump(%__MODULE__{} = message) do
    %{
      invalidation_id: message.invalidation_id,
      tenant_ref: message.tenant_ref,
      topic: message.topic,
      source_node_ref: message.source_node_ref,
      commit_lsn: message.commit_lsn,
      commit_hlc: message.commit_hlc,
      published_at: DateTime.to_iso8601(message.published_at),
      metadata: message.metadata
    }
  end

  @spec publish(t() | map() | keyword()) :: :ok | {:error, term()}
  def publish(%__MODULE__{} = message), do: configured_publish(message)

  def publish(attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, message} <- new(attrs) do
      publish(message)
    end
  end

  @spec cache_fanout_topic() :: String.t()
  def cache_fanout_topic, do: @cache_fanout_topic

  @spec hash_segment(String.t()) :: String.t()
  def hash_segment(ref) do
    ref
    |> required_string(:ref)
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> binary_part(0, 16)
  end

  @spec policy_topic!(keyword()) :: String.t()
  def policy_topic!(opts) when is_list(opts) do
    tenant_ref = Keyword.get(opts, :tenant_ref) || @global_tenant_ref
    installation_ref = Keyword.get(opts, :installation_ref) || @global_installation_ref
    kind = opts |> Keyword.fetch!(:kind) |> kind_segment!()
    policy_id = Keyword.fetch!(opts, :policy_id)
    version = opts |> Keyword.fetch!(:version) |> positive_integer!(:version)

    topic!([
      "memory",
      "policy",
      hash_segment(tenant_ref),
      hash_segment(installation_ref),
      kind,
      hash_segment(policy_id),
      Integer.to_string(version)
    ])
  end

  @spec graph_topic!(String.t(), pos_integer()) :: String.t()
  def graph_topic!(tenant_ref, epoch) do
    topic!(["memory", "graph", hash_segment(tenant_ref), "epoch", Integer.to_string(epoch)])
  end

  @spec fragment_topic!(String.t(), String.t()) :: String.t()
  def fragment_topic!(tenant_ref, fragment_id) do
    topic!(["memory", "fragment", hash_segment(tenant_ref), hash_segment(fragment_id)])
  end

  @spec invalidation_topic!(String.t(), String.t()) :: String.t()
  def invalidation_topic!(tenant_ref, invalidation_id) do
    topic!(["memory", "invalidation", hash_segment(tenant_ref), hash_segment(invalidation_id)])
  end

  defp configured_publish(%__MODULE__{} = message) do
    case Application.get_env(:mezzanine_config_registry, :cluster_invalidation_publisher) do
      nil ->
        telemetry_publish(message)

      {:phoenix_pubsub, pubsub_name} ->
        phoenix_pubsub_publish(pubsub_name, message)

      {module, function} when is_atom(module) and is_atom(function) ->
        apply(module, function, [message])

      {module, function, extra_args}
      when is_atom(module) and is_atom(function) and is_list(extra_args) ->
        apply(module, function, [message | extra_args])

      module when is_atom(module) ->
        module.publish(message)
    end
  end

  defp phoenix_pubsub_publish(pubsub_name, %__MODULE__{} = message) do
    with :ok <-
           Phoenix.PubSub.broadcast(
             pubsub_name,
             message.topic,
             {:cluster_invalidation, message}
           ),
         :ok <-
           Phoenix.PubSub.broadcast(
             pubsub_name,
             @cache_fanout_topic,
             {:cluster_invalidation, message}
           ) do
      telemetry_publish(message)
    end
  end

  defp telemetry_publish(%__MODULE__{} = message) do
    :telemetry.execute(@telemetry_event, %{count: 1}, %{
      topic: message.topic,
      message: message
    })

    :ok
  end

  defp topic!(segments) when is_list(segments) do
    segments
    |> Enum.map_join(".", &segment!/1)
    |> topic!(:topic)
  end

  defp topic!(topic, key) when is_binary(topic) do
    if Regex.match?(@topic_regex, topic) do
      topic
    else
      raise ArgumentError, "#{field(key)} must use lowercase ASCII topic segments"
    end
  end

  defp topic!(topic, key) do
    raise ArgumentError, "#{field(key)} must be a topic string, got: #{inspect(topic)}"
  end

  defp segment!(segment) do
    segment = required_string(segment, :topic_segment)

    if Regex.match?(@topic_segment_regex, segment) do
      segment
    else
      raise ArgumentError, "#{field(:topic_segment)} is invalid: #{inspect(segment)}"
    end
  end

  defp kind_segment!(kind) when is_atom(kind), do: kind |> Atom.to_string() |> kind_segment!()

  defp kind_segment!(kind) when is_binary(kind) do
    kind
    |> String.downcase()
    |> String.replace("_", "-")
    |> segment!()
    |> String.replace("-", "_")
  end

  defp kind_segment!(kind),
    do: raise(ArgumentError, "#{field(:kind)} is invalid: #{inspect(kind)}")

  defp commit_hlc!(value) when is_map(value) do
    normalized = %{
      "w" => integer_field!(value, "w"),
      "l" => integer_field!(value, "l"),
      "n" => required_string(map_get(value, "n"), :commit_hlc_node)
    }

    if normalized["w"] >= 0 and normalized["l"] >= 0 do
      normalized
    else
      raise ArgumentError, "#{field(:commit_hlc)} counters must be non-negative"
    end
  end

  defp commit_hlc!(value) do
    raise ArgumentError, "#{field(:commit_hlc)} must be an HLC map, got: #{inspect(value)}"
  end

  defp datetime!(%DateTime{} = value, _key), do: value

  defp datetime!(value, key) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _error -> raise ArgumentError, "#{field(key)} must be a DateTime"
    end
  end

  defp datetime!(value, key) do
    raise ArgumentError, "#{field(key)} must be a DateTime, got: #{inspect(value)}"
  end

  defp metadata!(nil), do: %{}
  defp metadata!(value) when is_map(value), do: value

  defp metadata!(value) do
    raise ArgumentError, "#{field(:metadata)} must be a map, got: #{inspect(value)}"
  end

  defp positive_integer!(value, _key) when is_integer(value) and value > 0, do: value

  defp positive_integer!(value, key) do
    raise ArgumentError, "#{field(key)} must be a positive integer, got: #{inspect(value)}"
  end

  defp integer_field!(map, key) do
    case map_get(map, key) do
      value when is_integer(value) ->
        value

      value ->
        raise ArgumentError,
              "#{field(:commit_hlc)}.#{key} must be an integer, got: #{inspect(value)}"
    end
  end

  defp required_string(value, key) when is_binary(value) do
    case String.trim(value) do
      "" -> raise ArgumentError, "#{field(key)} must be non-empty"
      trimmed -> trimmed
    end
  end

  defp required_string(value, key) do
    raise ArgumentError, "#{field(key)} must be a string, got: #{inspect(value)}"
  end

  defp fetch(map, key), do: Map.get(map, key) || Map.get(map, to_string(key))

  defp map_get(map, key) when is_binary(key),
    do: Map.get(map, key) || Map.get(map, Map.get(@commit_hlc_key_lookup, key))

  defp field(key), do: "cluster_invalidation.#{key}"
end
