defmodule Mezzanine.IntegrationBridgeTest do
  use ExUnit.Case, async: true

  alias Mezzanine.IntegrationBridge
  alias MezzanineOpsModel.Intent.{EffectIntent, ReadIntent, RunIntent}

  test "invoke_run_intent delegates to the public integration facade shape" do
    intent =
      RunIntent.new!(%{
        intent_id: "intent-run-1",
        program_id: "program-1",
        work_id: "work-1",
        capability: "linear.issue.execute",
        input: %{"issue_id" => "ENG-42"}
      })

    invoke_fun = fn capability, input, opts ->
      send(self(), {:invoke, capability, input, opts})
      {:ok, %{capability: capability, input: input}}
    end

    assert {:ok, %{capability: "linear.issue.execute"}} =
             IntegrationBridge.invoke_run_intent(
               intent,
               invoke_fun: invoke_fun,
               invoke_opts: [connection_id: "conn-1"]
             )

    assert_received {:invoke, "linear.issue.execute", %{"issue_id" => "ENG-42"},
                     [connection_id: "conn-1"]}
  end

  test "dispatch_effect invokes a capability-backed effect" do
    intent =
      EffectIntent.new!(%{
        intent_id: "effect-1",
        effect_type: :connector_effect,
        subject: "issue",
        payload: %{
          capability_id: "linear.issue.update",
          input: %{"id" => "ENG-42", "state" => "done"}
        }
      })

    invoke_fun = fn capability, input, _opts ->
      {:ok, %{capability: capability, input: input}}
    end

    assert {:ok, %{capability: "linear.issue.update"}} =
             IntegrationBridge.dispatch_effect(intent, invoke_fun: invoke_fun)
  end

  test "dispatch_read invokes a capability-backed read" do
    intent =
      ReadIntent.new!(%{
        intent_id: "read-1",
        read_type: :connector_read,
        subject: "issue",
        query: %{
          capability_id: "linear.issue.read",
          issue_id: "ENG-42"
        }
      })

    invoke_fun = fn capability, input, _opts ->
      {:ok, %{capability: capability, input: input}}
    end

    assert {:ok, %{capability: "linear.issue.read"}} =
             IntegrationBridge.dispatch_read(intent, invoke_fun: invoke_fun)
  end

  test "event translation maps direct platform outcomes to audit attrs" do
    mapped =
      IntegrationBridge.to_audit_attrs(
        %{status: :started, run_id: "run-1", payload: %{"attempt" => 1}},
        %{program_id: "program-1", work_object_id: "work-1"}
      )

    assert mapped.event_kind == :run_started
    assert mapped.program_id == "program-1"
    assert mapped.work_object_id == "work-1"
  end
end
