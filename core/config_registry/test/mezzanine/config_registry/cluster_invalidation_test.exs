defmodule Mezzanine.ConfigRegistry.ClusterInvalidationTest do
  use ExUnit.Case, async: false

  alias Mezzanine.ConfigRegistry.ClusterInvalidation

  @node_ref "node://mez_1@127.0.0.1/node-a"
  @commit_hlc %{
    "w" => 1_776_947_200_000_000_000,
    "l" => 0,
    "n" => @node_ref
  }

  test "encodes typed refs as bounded lowercase topic segments" do
    topic =
      ClusterInvalidation.policy_topic!(
        tenant_ref: "tenant://alpha",
        installation_ref: "installation://app-a",
        kind: :read,
        policy_id: "policy://read/default",
        version: 7
      )

    assert topic ==
             "memory.policy.#{ClusterInvalidation.hash_segment("tenant://alpha")}.#{ClusterInvalidation.hash_segment("installation://app-a")}.read.#{ClusterInvalidation.hash_segment("policy://read/default")}.7"

    for segment <- String.split(topic, ".") do
      assert segment =~ ~r/\A[a-z0-9_-]+\z/
      refute String.contains?(segment, "://")
    end
  end

  test "builds message with ordering evidence and rejects invalid topics" do
    topic = ClusterInvalidation.fragment_topic!("tenant://alpha", "fragment://memory/private-a")

    assert {:ok, message} =
             ClusterInvalidation.new(%{
               invalidation_id: "invalidation://memory/private-a",
               tenant_ref: "tenant://alpha",
               topic: topic,
               source_node_ref: @node_ref,
               commit_lsn: "16/B374D848",
               commit_hlc: @commit_hlc,
               published_at: ~U[2026-04-23 12:00:00Z]
             })

    assert message.topic == topic
    assert message.commit_lsn == "16/B374D848"
    assert message.commit_hlc == @commit_hlc

    assert {:error, %ArgumentError{} = error} =
             ClusterInvalidation.new(%{
               invalidation_id: "bad",
               tenant_ref: "tenant://alpha",
               topic: "memory.fragment.tenant://alpha.fragment-a",
               source_node_ref: @node_ref,
               commit_lsn: "16/B374D848",
               commit_hlc: @commit_hlc,
               published_at: ~U[2026-04-23 12:00:00Z]
             })

    assert error.message =~ "cluster_invalidation.topic"
  end

  test "default publisher emits the normalized invalidation message through telemetry" do
    ref = make_ref()
    handler_id = {__MODULE__, self(), ref}

    :telemetry.attach(
      handler_id,
      [:mezzanine, :cluster_invalidation, :publish],
      fn event, measurements, metadata, _config ->
        send(self(), {:cluster_invalidation, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    message =
      ClusterInvalidation.new!(%{
        invalidation_id: "invalidation://policy/read-default",
        tenant_ref: "tenant://alpha",
        topic:
          ClusterInvalidation.policy_topic!(
            tenant_ref: "tenant://alpha",
            installation_ref: "installation://app-a",
            kind: :read,
            policy_id: "policy://read/default",
            version: 1
          ),
        source_node_ref: @node_ref,
        commit_lsn: "16/B374D848",
        commit_hlc: @commit_hlc,
        published_at: ~U[2026-04-23 12:00:00Z]
      })

    assert :ok = ClusterInvalidation.publish(message)

    assert_receive {:cluster_invalidation, [:mezzanine, :cluster_invalidation, :publish],
                    %{count: 1}, %{message: published}}

    assert published.topic == message.topic
    assert published.invalidation_id == "invalidation://policy/read-default"
  end

  test "configured Phoenix PubSub publisher broadcasts exact and cache fanout topics" do
    pubsub = Module.concat(__MODULE__, PubSub)
    start_supervised!({Phoenix.PubSub, name: pubsub})

    previous_publisher =
      Application.get_env(:mezzanine_config_registry, :cluster_invalidation_publisher)

    Application.put_env(
      :mezzanine_config_registry,
      :cluster_invalidation_publisher,
      {:phoenix_pubsub, pubsub}
    )

    on_exit(fn ->
      if is_nil(previous_publisher) do
        Application.delete_env(:mezzanine_config_registry, :cluster_invalidation_publisher)
      else
        Application.put_env(
          :mezzanine_config_registry,
          :cluster_invalidation_publisher,
          previous_publisher
        )
      end
    end)

    message =
      ClusterInvalidation.new!(%{
        invalidation_id: "invalidation://policy/read-default",
        tenant_ref: "tenant://alpha",
        topic:
          ClusterInvalidation.policy_topic!(
            tenant_ref: "tenant://alpha",
            installation_ref: "installation://app-a",
            kind: :read,
            policy_id: "policy://read/default",
            version: 1
          ),
        source_node_ref: @node_ref,
        commit_lsn: "16/B374D848",
        commit_hlc: @commit_hlc,
        published_at: ~U[2026-04-23 12:00:00Z]
      })

    :ok = Phoenix.PubSub.subscribe(pubsub, message.topic)
    :ok = Phoenix.PubSub.subscribe(pubsub, ClusterInvalidation.cache_fanout_topic())

    assert :ok = ClusterInvalidation.publish(message)

    assert_receive {:cluster_invalidation, ^message}
    assert_receive {:cluster_invalidation, ^message}
  end
end
