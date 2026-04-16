defmodule Mezzanine.Boundary.GenerationManifestTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Boundary.GenerationManifest

  test "freezes the generated artifact families for every northbound service" do
    assert [:work_queries, :work_control, :operator_actions, :reviews, :installations] ==
             GenerationManifest.service_families()

    for family <- GenerationManifest.service_families() do
      spec = GenerationManifest.fetch!(family)

      assert :backend_behaviour in spec.generated_artifacts
      assert :request_mapper in spec.generated_artifacts
      assert :response_mapper in spec.generated_artifacts
      assert :fixture_builder in spec.generated_artifacts
    end
  end

  test "pack logic and policy hooks stay manual even when scaffolding is generated" do
    assert [:policy_hooks, :pack_specific_projection_logic, :semantic_adapters] ==
             GenerationManifest.manual_extension_points()

    refute :policy_hooks in GenerationManifest.generated_artifacts()
    refute :pack_specific_projection_logic in GenerationManifest.generated_artifacts()
    refute :semantic_adapters in GenerationManifest.generated_artifacts()
  end
end
