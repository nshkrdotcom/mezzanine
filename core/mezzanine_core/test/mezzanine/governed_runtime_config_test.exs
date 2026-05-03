defmodule Mezzanine.GovernedRuntimeConfigTest do
  use ExUnit.Case, async: false

  alias Mezzanine.GovernedRuntimeConfig

  defmodule EnvSelectedRuntime, do: nil
  defmodule ExplicitRuntime, do: nil
  defmodule DefaultRuntime, do: nil

  setup do
    previous = Application.get_env(:mezzanine_core, :workflow_runtime_impl)
    Application.put_env(:mezzanine_core, :workflow_runtime_impl, EnvSelectedRuntime)

    on_exit(fn ->
      if previous do
        Application.put_env(:mezzanine_core, :workflow_runtime_impl, previous)
      else
        Application.delete_env(:mezzanine_core, :workflow_runtime_impl)
      end
    end)
  end

  test "explicit runtime modules win over application config" do
    assert GovernedRuntimeConfig.module(
             %{runtime_modules: %{workflow_runtime_impl: ExplicitRuntime}},
             :mezzanine_core,
             :workflow_runtime_impl,
             DefaultRuntime
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

  test "standalone compatibility can still use application config" do
    assert GovernedRuntimeConfig.module(
             %{},
             :mezzanine_core,
             :workflow_runtime_impl,
             DefaultRuntime
           ) == EnvSelectedRuntime
  end
end
