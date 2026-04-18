Code.require_file("internal_modularity_contract.exs", __DIR__)

alias Mezzanine.Build.InternalModularityContract

errors =
  InternalModularityContract.package_specs()
  |> Enum.flat_map(fn %{path: package_path, allowed_internal_deps: allowed_internal_deps} ->
    actual_internal_deps = InternalModularityContract.declared_internal_deps(package_path)

    missing =
      allowed_internal_deps
      |> Enum.reject(&(&1 in actual_internal_deps))

    unexpected =
      actual_internal_deps
      |> Enum.reject(&(&1 in allowed_internal_deps))

    []
    |> then(fn errors ->
      case missing do
        [] -> errors
        _ -> [{package_path, :missing_internal_deps, missing} | errors]
      end
    end)
    |> then(fn errors ->
      case unexpected do
        [] -> errors
        _ -> [{package_path, :unexpected_internal_deps, unexpected} | errors]
      end
    end)
  end)

case errors do
  [] ->
    IO.puts(
      "mezzanine internal modularity contract passed for #{length(InternalModularityContract.package_specs())} core packages"
    )

  _errors ->
    IO.puts("mezzanine internal modularity contract failed")

    Enum.each(errors, fn {package_path, kind, deps} ->
      IO.puts("  #{package_path} #{kind}: #{Enum.join(deps, ", ")}")
    end)

    System.halt(1)
end
