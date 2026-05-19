defmodule Mezzanine.RuntimeProfileStoreTest do
  use ExUnit.Case, async: true

  alias Mezzanine.RuntimeProfile
  alias Mezzanine.RuntimeProfileStore

  test "owns a supplied runtime profile under supervision" do
    profile =
      RuntimeProfile.empty()
      |> RuntimeProfile.put(:mezzanine_leasing, :default_read_ttl_ms, 123)

    pid = start_supervised!({RuntimeProfileStore, name: nil, profile: profile})

    assert RuntimeProfileStore.config(:mezzanine_leasing, :default_read_ttl_ms, 300_000, pid) ==
             123
  end

  test "replaces profile explicitly and returns the previous value" do
    first =
      RuntimeProfile.empty()
      |> RuntimeProfile.put(:mezzanine_leasing, :default_read_ttl_ms, 123)

    second =
      RuntimeProfile.empty()
      |> RuntimeProfile.put(:mezzanine_leasing, :default_read_ttl_ms, 456)

    pid = start_supervised!({RuntimeProfileStore, name: nil, profile: first})

    assert {:ok, ^first} = RuntimeProfileStore.replace_profile(second, pid)

    assert RuntimeProfileStore.config(:mezzanine_leasing, :default_read_ttl_ms, 300_000, pid) ==
             456
  end

  test "falls back to an empty profile when no owner is running" do
    assert RuntimeProfileStore.config(
             :mezzanine_leasing,
             :default_read_ttl_ms,
             300_000,
             :missing_runtime_profile_store
           ) ==
             300_000
  end
end
