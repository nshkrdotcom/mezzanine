defmodule Mezzanine.RuntimeProfileTest do
  use ExUnit.Case, async: true

  alias Mezzanine.RuntimeProfile

  defmodule RuntimeImpl, do: nil

  test "captures only known runtime profile keys from snapshots" do
    profile =
      RuntimeProfile.from_env_snapshot(%{
        mezzanine_core: [
          workflow_runtime_impl: RuntimeImpl,
          unrelated: :ignored
        ],
        unknown_app: [workflow_runtime_impl: :ignored]
      })

    assert RuntimeProfile.module(
             profile,
             :mezzanine_core,
             :workflow_runtime_impl,
             DefaultRuntime
           ) == RuntimeImpl

    assert RuntimeProfile.config(profile, :mezzanine_leasing, :default_read_ttl_ms, 300_000) ==
             300_000
  end

  test "rejects unknown profile keys at explicit write boundaries" do
    assert_raise ArgumentError,
                 "unknown Mezzanine runtime profile key {:mezzanine_core, :unknown_key}",
                 fn ->
                   RuntimeProfile.put(
                     RuntimeProfile.empty(),
                     :mezzanine_core,
                     :unknown_key,
                     :value
                   )
                 end
  end

  test "returns keyword config only when the configured value is a keyword list" do
    profile =
      RuntimeProfile.empty()
      |> RuntimeProfile.put(:mezzanine_workflow_runtime, :temporal, enabled?: true)
      |> RuntimeProfile.put(:mezzanine_leasing, :default_read_ttl_ms, 42)

    assert RuntimeProfile.keyword_config(profile, :mezzanine_workflow_runtime, :temporal, []) ==
             [enabled?: true]

    assert RuntimeProfile.keyword_config(profile, :mezzanine_leasing, :default_read_ttl_ms, []) ==
             []
  end
end
