Code.require_file("projected_artifact_fidelity.exs", __DIR__)

alias Mezzanine.Build.ProjectedArtifactFidelity

profile = ProjectedArtifactFidelity.profile()
ProjectedArtifactFidelity.validate!(profile, require_projection?: true)

IO.puts(
  "mezzanine projected artifact fidelity passed for #{profile.artifact_id}; " <>
    "source_roots=#{length(profile.source_root_ref_set)} " <>
    "projection_ref=#{profile.projection_branch_or_archive_ref} " <>
    "artifact_hash_manifest_ref=#{profile.artifact_hash_manifest_ref}"
)
