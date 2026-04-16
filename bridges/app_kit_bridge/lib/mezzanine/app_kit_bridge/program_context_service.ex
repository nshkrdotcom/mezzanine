defmodule Mezzanine.AppKitBridge.ProgramContextService do
  @moduledoc """
  Resolves durable routing identifiers from product-owned metadata.

  App-facing callers should not need to carry lower `program_id` and
  `work_class_id` values just to use the typed `app_kit` surfaces. This
  service keeps that lookup inside the lower bridge seam.
  """

  alias Mezzanine.AppKitBridge.AdapterSupport
  alias Mezzanine.Programs.Program
  alias Mezzanine.Work.WorkClass

  @spec resolve(String.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def resolve(tenant_id, attrs, opts \\ [])
      when is_binary(tenant_id) and is_map(attrs) and is_list(opts) do
    attrs = Map.new(attrs)

    with {:ok, program_slug} <- fetch_string(attrs, opts, :program_slug, :missing_program_slug),
         {:ok, program} <- resolve_program(tenant_id, program_slug) do
      work_class_context(program, tenant_id, optional_string(attrs, opts, :work_class_name))
    else
      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp work_class_context(%Program{} = program, _tenant_id, value)
       when not is_binary(value) or value == "" do
    {:ok, %{program_id: program.id}}
  end

  defp work_class_context(%Program{} = program, tenant_id, work_class_name) do
    case resolve_work_class(tenant_id, program.id, work_class_name) do
      {:ok, work_class_id} ->
        {:ok, %{program_id: program.id, work_class_id: work_class_id}}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp resolve_program(tenant_id, program_slug) do
    case Program.by_slug(tenant_id, program_slug, actor: actor(tenant_id), tenant: tenant_id) do
      {:ok, %Program{} = program} -> {:ok, program}
      {:error, _reason} -> {:error, :bridge_not_found}
    end
  end

  defp resolve_work_class(tenant_id, program_id, work_class_name) do
    case WorkClass.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id) do
      {:ok, work_classes} ->
        case Enum.find(work_classes, &(&1.name == work_class_name)) do
          %WorkClass{id: work_class_id} -> {:ok, work_class_id}
          nil -> {:error, :bridge_not_found}
        end

      {:error, _reason} ->
        {:error, :bridge_not_found}
    end
  end

  defp fetch_string(attrs, opts, key, error),
    do: AdapterSupport.fetch_string(attrs, opts, key, error)

  defp optional_string(attrs, opts, key),
    do: AdapterSupport.optional_string(attrs, opts, key)

  defp actor(tenant_id), do: AdapterSupport.actor(tenant_id)
  defp normalize_error(reason), do: AdapterSupport.normalize_error(reason)
end
