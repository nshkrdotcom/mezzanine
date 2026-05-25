defmodule Mezzanine.Bridges.JidoIntegrationBridge.TransportTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Bridges.JidoIntegrationBridge.Transport

  defmodule DirectTarget do
    def submit_invocation(request, opts) do
      {:ok, %{"mode" => "direct", "request" => request, "timeout" => opts[:timeout]}}
    end

    def read_invocation(ref), do: {:ok, %{"mode" => "direct", "ref" => ref}}
  end

  test "direct transport calls an explicitly supplied Jido facade" do
    assert {:ok, result} =
             Transport.Direct.submit_invocation(%{"prompt_artifact_ref" => "prompt://one"},
               target: DirectTarget,
               timeout: 50
             )

    assert result["mode"] == "direct"
    assert result["timeout"] == 50
  end

  test "distributed transport calls an explicitly supplied Jido facade" do
    assert {:ok, result} =
             Transport.Distributed.submit_invocation(%{"prompt_artifact_ref" => "prompt://one"},
               node: Node.self(),
               facade_module: DirectTarget,
               timeout: 1_000
             )

    assert result["mode"] == "direct"
  end

  test "fixture transport provides deterministic invocation evidence" do
    assert {:ok, result} = Transport.Fixture.submit_invocation(%{}, [])
    assert result["invocation_ref"] == "invocation://fixture/jido"
  end

  test "runtime deps select a Jido transport explicitly" do
    assert {:ok, deps} =
             Transport.RuntimeDeps.new(
               transport: Transport.Fixture,
               transport_opts: [submit_invocation: {:ok, %{"status" => "accepted"}}]
             )

    assert {:ok, %{"status" => "accepted"}} = Transport.RuntimeDeps.submit_invocation(deps, %{})
  end
end
