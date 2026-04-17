defmodule Mezzanine.Policy.BundleLoaderTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Policy.BundleLoader
  alias Mezzanine.Policy.Compiler
  alias Mezzanine.Policy.TypedConfig
  alias MezzanineOpsModel.PlacementProfile

  @fixture_path Path.expand("../../fixtures/workflow.md", __DIR__)

  test "loads a workflow markdown file with YAML front matter and prompt body" do
    assert {:ok, bundle} = BundleLoader.load_file(@fixture_path)

    assert bundle.source_ref == @fixture_path
    assert bundle.config["tracker"]["kind"] == "linear"
    assert bundle.prompt_template =~ "Operate on the assigned work."
  end

  test "compiles typed config from a realistic workflow bundle" do
    assert {:ok, bundle} = BundleLoader.load_file(@fixture_path)
    assert {:ok, compiled_bundle} = Compiler.compile(bundle)

    assert TypedConfig.run_profile(compiled_bundle) == %{
             profile_id: "default_session",
             runtime_class: :session,
             capability: "linear.issue.execute",
             target: "linear-default"
           }

    assert TypedConfig.approval_posture(compiled_bundle) == %{
             mode: :manual,
             reviewers: ["ops_lead", "duty_engineer"],
             escalation_required: true
           }

    assert TypedConfig.retry_profile(compiled_bundle) == %{
             strategy: :exponential,
             max_attempts: 4,
             initial_backoff_ms: 5_000,
             max_backoff_ms: 300_000
           }

    assert %PlacementProfile{} = TypedConfig.placement_profile(compiled_bundle)
    assert TypedConfig.review_rules(compiled_bundle).required

    assert Enum.map(TypedConfig.capability_grants(compiled_bundle), & &1.capability_id) == [
             "linear.issue.read",
             "linear.issue.update"
           ]
  end

  test "keeps the last known good compiled bundle on reload failure" do
    assert {:ok, bundle} = BundleLoader.load_file(@fixture_path)
    assert {:ok, compiled_bundle} = Compiler.compile(bundle)

    assert {:fallback, ^compiled_bundle, :workflow_front_matter_not_a_map} =
             BundleLoader.reload_string(compiled_bundle, "---\n- nope\n---\nbody")
  end

  test "reads persisted string-key compiled bundles through the same typed getters" do
    assert {:ok, bundle} = BundleLoader.load_file(@fixture_path)
    assert {:ok, compiled_bundle} = Compiler.compile(bundle)

    persisted_bundle = %{
      compiled_bundle
      | compiled_form:
          compiled_bundle.compiled_form
          |> Enum.into(%{}, fn {key, value} -> {Atom.to_string(key), stringify_keys(value)} end)
    }

    assert TypedConfig.run_profile(persisted_bundle).capability == "linear.issue.execute"
    assert TypedConfig.review_rules(persisted_bundle).required
    assert TypedConfig.placement_profile(persisted_bundle).profile_id == "default-placement"

    assert Enum.map(TypedConfig.capability_grants(persisted_bundle), & &1.capability_id) == [
             "linear.issue.read",
             "linear.issue.update"
           ]
  end

  test "coerces persisted boolean strings in compiled bundles" do
    persisted_bundle =
      MezzanineOpsModel.PolicyBundle.new!(%{
        bundle_id: "policy-legacy",
        source_ref: "WORKFLOW.md",
        config: %{},
        prompt_template: "Do the work.",
        compiled_form: %{
          "approval_posture" => %{
            "mode" => "manual",
            "reviewers" => ["ops_lead"],
            "escalation_required" => "false"
          },
          "review_rules" => %{
            "required" => "false",
            "required_decisions" => 0,
            "gates" => ["operator"]
          },
          "run_profile" => %{
            "profile_id" => "default_session",
            "runtime_class" => "session",
            "capability" => "linear.issue.execute",
            "target" => "linear-default"
          },
          "retry_profile" => %{
            "strategy" => "exponential",
            "max_attempts" => 4,
            "initial_backoff_ms" => 5_000,
            "max_backoff_ms" => 300_000
          },
          "placement_profile" => %{
            "profile_id" => "default-placement",
            "strategy" => "affinity",
            "target_selector" => %{},
            "runtime_preferences" => %{},
            "workspace_policy" => %{"reuse" => "true"},
            "metadata" => %{}
          },
          "capability_grants" => [
            %{
              "capability_id" => "linear.issue.read",
              "mode" => "allow",
              "constraints" => %{}
            }
          ]
        }
      })

    assert TypedConfig.run_profile(persisted_bundle).runtime_class == :session
    assert TypedConfig.approval_posture(persisted_bundle).mode == :manual
    assert TypedConfig.approval_posture(persisted_bundle).escalation_required == false
    assert TypedConfig.retry_profile(persisted_bundle).strategy == :exponential
    assert TypedConfig.review_rules(persisted_bundle).required == false
    assert TypedConfig.placement_profile(persisted_bundle).strategy == :affinity
    assert TypedConfig.placement_profile(persisted_bundle).workspace_policy["reuse"] == true
    assert Enum.map(TypedConfig.capability_grants(persisted_bundle), & &1.mode) == [:allow]
  end

  test "rejects malformed grant declarations during compile" do
    assert {:ok, bundle} =
             BundleLoader.load_map(%{
               config: %{
                 "capability_grants" => [
                   %{"capability_id" => "linear.issue.read", "mode" => "wild"}
                 ]
               },
               prompt_template: "Do the work."
             })

    assert {:error, {:invalid_grant_mode, "wild"}} = Compiler.compile(bundle)
  end

  defp stringify_keys(%_struct{} = value) do
    value
    |> Map.from_struct()
    |> stringify_keys()
  end

  defp stringify_keys(value) when is_map(value) do
    Enum.into(value, %{}, fn {key, item} -> {to_string(key), stringify_keys(item)} end)
  end

  defp stringify_keys(value) when is_list(value), do: Enum.map(value, &stringify_keys/1)
  defp stringify_keys(value), do: value
end
