defmodule Mezzanine.Build.InternalModularityContract do
  @moduledoc false

  @root Path.expand("..", __DIR__)

  @package_specs [
    %{path: "core/mezzanine_core", allowed_internal_deps: []},
    %{path: "core/ops_model", allowed_internal_deps: []},
    %{path: "core/ops_domain", allowed_internal_deps: ["core/ops_model"]},
    %{path: "core/pack_model", allowed_internal_deps: []},
    %{path: "core/pack_compiler", allowed_internal_deps: ["core/pack_model"]},
    %{
      path: "core/barriers",
      allowed_internal_deps: ["core/execution_engine", "core/mezzanine_core"]
    },
    %{path: "core/leasing", allowed_internal_deps: ["core/mezzanine_core"]},
    %{
      path: "core/config_registry",
      allowed_internal_deps: [
        "core/execution_engine",
        "core/leasing",
        "core/pack_compiler",
        "core/pack_model"
      ]
    },
    %{path: "core/audit_engine", allowed_internal_deps: ["core/ops_domain"]},
    %{path: "core/object_engine", allowed_internal_deps: ["core/audit_engine"]},
    %{
      path: "core/execution_engine",
      allowed_internal_deps: [
        "core/audit_engine",
        "core/leasing",
        "core/mezzanine_core",
        "core/object_engine",
        "core/ops_domain"
      ]
    },
    %{
      path: "core/lifecycle_engine",
      allowed_internal_deps: [
        "core/audit_engine",
        "core/barriers",
        "core/config_registry",
        "core/execution_engine",
        "core/mezzanine_core",
        "core/object_engine",
        "core/pack_compiler"
      ]
    },
    %{
      path: "core/runtime_scheduler",
      allowed_internal_deps: [
        "core/audit_engine",
        "core/execution_engine",
        "core/lifecycle_engine",
        "core/mezzanine_core",
        "core/object_engine"
      ]
    },
    %{path: "core/workflow_runtime", allowed_internal_deps: []},
    %{
      path: "core/decision_engine",
      allowed_internal_deps: ["core/audit_engine", "core/execution_engine", "core/object_engine"]
    },
    %{
      path: "core/evidence_engine",
      allowed_internal_deps: ["core/audit_engine", "core/execution_engine", "core/object_engine"]
    },
    %{
      path: "core/projection_engine",
      allowed_internal_deps: [
        "core/audit_engine",
        "core/decision_engine",
        "core/evidence_engine",
        "core/execution_engine",
        "core/object_engine"
      ]
    },
    %{
      path: "core/operator_engine",
      allowed_internal_deps: [
        "core/execution_engine",
        "core/leasing",
        "core/mezzanine_core",
        "core/object_engine"
      ]
    },
    %{
      path: "core/archival_engine",
      allowed_internal_deps: [
        "core/audit_engine",
        "core/config_registry",
        "core/decision_engine",
        "core/evidence_engine",
        "core/execution_engine",
        "core/mezzanine_core",
        "core/object_engine"
      ]
    }
  ]

  @package_paths MapSet.new(Enum.map(@package_specs, & &1.path))

  @spec root() :: String.t()
  def root, do: @root

  @spec package_specs() :: [map()]
  def package_specs, do: @package_specs

  @spec package_paths() :: [String.t()]
  def package_paths do
    @package_specs
    |> Enum.map(& &1.path)
    |> Enum.sort()
  end

  @spec spec_for(String.t()) :: map() | nil
  def spec_for(path), do: Enum.find(@package_specs, &(&1.path == path))

  @spec declared_internal_deps(String.t()) :: [String.t()]
  def declared_internal_deps(package_path) when is_binary(package_path) do
    package_root = Path.join(@root, package_path)
    mix_file = Path.join(package_root, "mix.exs")

    mix_file
    |> File.read!()
    |> then(fn contents ->
      Regex.scan(~r/\{\s*:[a-z0-9_]+\s*,\s*path:\s*"([^"]+)"/, contents, capture: :all_but_first)
    end)
    |> Enum.map(fn [relative_path] -> normalize_internal_dep(package_root, relative_path) end)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp normalize_internal_dep(package_root, relative_path) do
    relative =
      package_root
      |> Path.join(relative_path)
      |> Path.expand()
      |> Path.relative_to(@root)

    if MapSet.member?(@package_paths, relative), do: relative, else: nil
  end
end
