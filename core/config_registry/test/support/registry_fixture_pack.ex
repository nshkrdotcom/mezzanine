defmodule Mezzanine.TestPacks.RegistryFixturePack do
  @moduledoc false
  @behaviour Mezzanine.Pack

  alias Mezzanine.Pack.{
    CompiledPack,
    Compiler,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    ProjectionSpec,
    SubjectKindSpec
  }

  @impl true
  def manifest do
    %Manifest{
      pack_slug: :expense_approval,
      version: "1.0.0",
      subject_kind_specs: [
        %SubjectKindSpec{name: :expense_request}
      ],
      lifecycle_specs: [
        %LifecycleSpec{
          subject_kind: :expense_request,
          initial_state: :submitted,
          terminal_states: [:paid],
          transitions: [
            %{
              from: :submitted,
              to: :processing,
              trigger: {:execution_requested, :expense_capture}
            },
            %{from: :processing, to: :paid, trigger: {:execution_completed, :expense_capture}}
          ]
        }
      ],
      execution_recipe_specs: [
        %ExecutionRecipeSpec{
          recipe_ref: :expense_capture,
          runtime_class: :session,
          placement_ref: :local_runner
        }
      ],
      projection_specs: [
        %ProjectionSpec{name: :active_expenses, subject_kinds: [:expense_request]}
      ]
    }
  end

  def compiled_pack! do
    case Compiler.compile(manifest()) do
      {:ok, %CompiledPack{} = compiled} -> compiled
      {:error, errors} -> raise "failed to compile registry fixture pack: #{inspect(errors)}"
    end
  end
end
