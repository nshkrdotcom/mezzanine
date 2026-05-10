weld_check = Path.expand("deps/weld/lib/weld/credo/check/no_runtime_os_env.ex", __DIR__)

if File.regular?(weld_check) and not Code.ensure_loaded?(Weld.Credo.Check.NoRuntimeOsEnv) do
  Code.require_file(weld_check)
end

%{
  configs: [
    %{
      name: "default",
      files: %{
        included: ["mix.exs", "lib/", "build_support/", "core/*/lib/", "bridges/*/lib/"],
        excluded: ["_build/", "deps/", "dist/", "doc/"]
      },
      checks: [
        {Weld.Credo.Check.NoRuntimeOsEnv, []}
      ]
    }
  ]
}
