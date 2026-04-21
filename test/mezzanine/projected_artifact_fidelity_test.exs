defmodule Mezzanine.ProjectedArtifactFidelityTest do
  use ExUnit.Case, async: true

  Code.require_file("../../build_support/projected_artifact_fidelity.exs", __DIR__)

  alias Mezzanine.Build.ProjectedArtifactFidelity
  alias Mezzanine.Build.WeldContract

  test "profile covers the required mezzanine_core projected artifact fidelity fields" do
    profile = ProjectedArtifactFidelity.profile(projection_path: "missing/projection")

    assert :ok = ProjectedArtifactFidelity.validate(profile)
    assert profile.artifact_id == "mezzanine_core"
    assert Enum.all?(ProjectedArtifactFidelity.required_fields(), &Map.has_key?(profile, &1))
    assert profile.release_manifest_ref == "mezzanine_projected_artifact_fidelity_profiles[0]"
    assert profile.weld_manifest_ref == "build_support/weld.exs"
    assert profile.weld_manifest_hash =~ ~r/\Asha256:[0-9a-f]{64}\z/
    assert profile.workspace_contract_ref =~ ~r/build_support\/workspace_contract\.exs:sha256:/

    assert profile.internal_modularity_contract_ref =~
             ~r/build_support\/internal_modularity_contract\.exs:sha256:/

    assert profile.weld_verify_or_packaging_test_ref ==
             "mix weld.verify -> packaging/weld/mezzanine_core/test"

    assert profile.manual_patch_disposition == "generated_from_source_roots"
    assert profile.owner_approval_ref_when_manual_patch == "not_required_no_manual_patch"

    assert Enum.map(profile.source_root_ref_set, & &1.path) ==
             Keyword.fetch!(WeldContract.artifact(), :roots)

    assert Enum.all?(profile.source_root_ref_set, fn root_ref ->
             root_ref.hash_ref =~ ~r/\Asha256:[0-9a-f]{64}\z/
           end)
  end

  test "requires a projected artifact hash manifest when the release gate asks for projection evidence" do
    profile = ProjectedArtifactFidelity.profile(projection_path: "missing/projection")

    assert {:error, errors} =
             ProjectedArtifactFidelity.validate(profile, require_projection?: true)

    assert {:missing_projected_artifact_hash_manifest,
            "missing_projection_archive_run_mix_weld_verify"} in errors
  end

  test "rejects manual projected artifact patches without owner approval evidence" do
    profile =
      ProjectedArtifactFidelity.profile(projection_path: "missing/projection")
      |> Map.put(:manual_patch_disposition, "owner_approved_emergency_patch_with_backport")

    assert {:error, errors} = ProjectedArtifactFidelity.validate(profile)

    assert {:owner_approval_required_for_manual_patch,
            "owner_approved_emergency_patch_with_backport"} in errors
  end
end
