defmodule Mezzanine.Memory.SharedScopeRegistryTest do
  use Mezzanine.ConfigRegistry.DataCase, async: false

  alias Mezzanine.ConfigRegistry.ClusterInvalidation
  alias Mezzanine.Memory.SharedScopeRegistry

  @tenant_ref "tenant://alpha"
  @scope_ref "scope://team-alpha"
  @node_ref "node://mez_1@127.0.0.1/node-a"
  @commit_hlc %{
    "w" => 1_776_947_200_000_000_000,
    "l" => 0,
    "n" => @node_ref
  }

  test "registry fails closed by default and evaluates visibility by epoch" do
    telemetry_id = attach_invalidation_telemetry()

    refute SharedScopeRegistry.scope_registered?(@scope_ref, 7, tenant_ref: @tenant_ref)

    assert :ok =
             SharedScopeRegistry.register(@scope_ref, governance_ref(),
               tenant_ref: @tenant_ref,
               activation_epoch: 8,
               source_node_ref: @node_ref,
               commit_hlc: @commit_hlc
             )

    refute SharedScopeRegistry.scope_registered?(@scope_ref, 7, tenant_ref: @tenant_ref)
    assert SharedScopeRegistry.scope_registered?(@scope_ref, 8, tenant_ref: @tenant_ref)

    assert_receive {:cluster_invalidation, %{message: registered_message}}

    assert registered_message.topic ==
             SharedScopeRegistry.invalidation_topic!(@tenant_ref, @scope_ref)

    assert registered_message.source_node_ref == @node_ref
    assert registered_message.commit_hlc == @commit_hlc
    assert registered_message.metadata["registry_action"] == "register"
    assert registered_message.metadata["activation_epoch"] == 8

    :telemetry.detach(telemetry_id)
  end

  test "deregister clears same-epoch cache and fails closed at the deregistration epoch" do
    assert :ok =
             SharedScopeRegistry.register(@scope_ref, governance_ref(),
               tenant_ref: @tenant_ref,
               activation_epoch: 8,
               source_node_ref: @node_ref,
               commit_hlc: @commit_hlc
             )

    assert SharedScopeRegistry.scope_registered?(@scope_ref, 12, tenant_ref: @tenant_ref)

    assert :ok =
             SharedScopeRegistry.deregister(@scope_ref, governance_ref(),
               tenant_ref: @tenant_ref,
               deregistration_epoch: 12,
               source_node_ref: @node_ref,
               commit_hlc: @commit_hlc
             )

    assert SharedScopeRegistry.scope_registered?(@scope_ref, 11, tenant_ref: @tenant_ref)
    refute SharedScopeRegistry.scope_registered?(@scope_ref, 12, tenant_ref: @tenant_ref)
  end

  test "registration and deregistration publish exact and shared cache fanout topics through Phoenix PubSub" do
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

    topic = SharedScopeRegistry.invalidation_topic!(@tenant_ref, @scope_ref)

    :ok = Phoenix.PubSub.subscribe(pubsub, topic)

    :ok =
      Phoenix.PubSub.subscribe(
        pubsub,
        ClusterInvalidation.cache_fanout_topic()
      )

    assert :ok =
             SharedScopeRegistry.register(@scope_ref, governance_ref(),
               tenant_ref: @tenant_ref,
               activation_epoch: 20,
               source_node_ref: @node_ref,
               commit_hlc: @commit_hlc
             )

    assert SharedScopeRegistry.scope_registered?(@scope_ref, 20, tenant_ref: @tenant_ref)

    assert_receive {:cluster_invalidation,
                    %{
                      topic: ^topic,
                      metadata: %{"registry_action" => "register"}
                    }}

    assert_receive {:cluster_invalidation,
                    %{
                      topic: ^topic,
                      metadata: %{"registry_action" => "register"}
                    }}

    deregister_node_ref = "node://mez_2@127.0.0.1/node-b"

    deregister_hlc = %{
      "w" => 1_776_947_200_000_000_100,
      "l" => 1,
      "n" => deregister_node_ref
    }

    assert :ok =
             SharedScopeRegistry.deregister(@scope_ref, governance_ref(),
               tenant_ref: @tenant_ref,
               deregistration_epoch: 21,
               source_node_ref: deregister_node_ref,
               commit_hlc: deregister_hlc
             )

    refute SharedScopeRegistry.scope_registered?(@scope_ref, 21, tenant_ref: @tenant_ref)

    assert_receive {:cluster_invalidation,
                    %{
                      topic: ^topic,
                      source_node_ref: ^deregister_node_ref,
                      commit_hlc: ^deregister_hlc,
                      metadata: %{"registry_action" => "deregister"}
                    }}

    assert_receive {:cluster_invalidation,
                    %{
                      topic: ^topic,
                      source_node_ref: ^deregister_node_ref,
                      commit_hlc: ^deregister_hlc,
                      metadata: %{"registry_action" => "deregister"}
                    }}
  end

  defp governance_ref do
    %{
      "ref" => "governance://shared-scope/team-alpha",
      "kind" => "shared_scope_registration",
      "id" => "team-alpha"
    }
  end

  defp attach_invalidation_telemetry do
    handler_id = {__MODULE__, self(), make_ref()}

    :telemetry.attach(
      handler_id,
      [:mezzanine, :cluster_invalidation, :publish],
      fn _event, _measurements, metadata, _config ->
        send(self(), {:cluster_invalidation, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    handler_id
  end
end
