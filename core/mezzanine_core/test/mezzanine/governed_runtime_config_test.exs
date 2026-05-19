defmodule Mezzanine.GovernedRuntimeConfigTest do
  use ExUnit.Case, async: false

  alias Mezzanine.GovernedRuntimeConfig
  alias Mezzanine.RuntimeProfile
  alias Mezzanine.RuntimeProfileStore

  defmodule ProfileSelectedRuntime, do: nil
  defmodule ExplicitRuntime, do: nil
  defmodule DefaultRuntime, do: nil

  test "explicit runtime modules win over application config" do
    profile =
      RuntimeProfile.empty()
      |> RuntimeProfile.put(:mezzanine_core, :workflow_runtime_impl, ProfileSelectedRuntime)

    assert GovernedRuntimeConfig.module(
             %{runtime_modules: %{workflow_runtime_impl: ExplicitRuntime}},
             :mezzanine_core,
             :workflow_runtime_impl,
             DefaultRuntime,
             runtime_profile: profile
           ) == ExplicitRuntime
  end

  test "governed requests use the compiled default instead of application config" do
    assert GovernedRuntimeConfig.module(
             %{authority_packet_ref: "authpkt-1", workflow_id: "workflow-1"},
             :mezzanine_core,
             :workflow_runtime_impl,
             DefaultRuntime,
             governed_default?: true
           ) == DefaultRuntime
  end

  test "standalone compatibility can use an explicit boot profile" do
    profile =
      RuntimeProfile.empty()
      |> RuntimeProfile.put(:mezzanine_core, :workflow_runtime_impl, ProfileSelectedRuntime)

    assert GovernedRuntimeConfig.module(
             %{},
             :mezzanine_core,
             :workflow_runtime_impl,
             DefaultRuntime,
             runtime_profile: profile
           ) == ProfileSelectedRuntime
  end

  test "standalone compatibility can use an explicit runtime profile store" do
    profile =
      RuntimeProfile.empty()
      |> RuntimeProfile.put(:mezzanine_core, :workflow_runtime_impl, ProfileSelectedRuntime)

    pid = start_supervised!({RuntimeProfileStore, name: nil, profile: profile})

    assert GovernedRuntimeConfig.module(
             %{},
             :mezzanine_core,
             :workflow_runtime_impl,
             DefaultRuntime,
             runtime_profile_store: pid
           ) == ProfileSelectedRuntime
  end
end
