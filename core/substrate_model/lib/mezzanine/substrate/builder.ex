defmodule Mezzanine.Substrate.Builder do
  @moduledoc false

  @type build_result :: {:ok, struct()} | {:error, term()}

  @spec build(module(), map() | keyword(), [atom()], keyword()) :: build_result()
  def build(module, attrs, required_fields, opts \\ []) when is_atom(module) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         :ok <- reject_forbidden_fields(attrs),
         :ok <- require_fields(attrs, required_fields),
         {:ok, attrs} <- validate_custom(attrs, opts) do
      {:ok, struct(module, attrs)}
    end
  end

  @spec normalize_attrs(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def normalize_attrs(attrs) when is_list(attrs), do: {:ok, Map.new(attrs)}
  def normalize_attrs(attrs) when is_map(attrs), do: {:ok, attrs}
  def normalize_attrs(_attrs), do: {:error, :invalid_attrs}

  @spec reject_forbidden_fields(map()) :: :ok | {:error, term()}
  def reject_forbidden_fields(attrs) when is_map(attrs) do
    case Enum.find(Map.keys(attrs), &forbidden_field?/1) do
      nil -> :ok
      field -> {:error, {:forbidden_generic_field, field}}
    end
  end

  defp validate_custom(attrs, opts) do
    opts
    |> Keyword.get(:validate, [])
    |> Enum.reduce_while({:ok, attrs}, fn validator, {:ok, current_attrs} ->
      case validator.(current_attrs) do
        :ok -> {:cont, {:ok, current_attrs}}
        {:ok, updated_attrs} -> {:cont, {:ok, updated_attrs}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp require_fields(attrs, required_fields) do
    case Enum.find(required_fields, &(not present?(Map.get(attrs, &1)))) do
      nil -> :ok
      field -> {:error, {:missing_required_field, field}}
    end
  end

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)

  defp forbidden_field?(field) when is_atom(field) do
    field in forbidden_field_atoms()
  end

  defp forbidden_field?(field) when is_binary(field) do
    field in forbidden_field_strings()
  end

  defp forbidden_field?(_field), do: false

  defp forbidden_field_atoms do
    forbidden_field_strings()
    |> Enum.map(&String.to_atom/1)
  end

  defp forbidden_field_strings do
    [
      "lin" <> "ear" <> "_" <> "issue" <> "_id",
      "lin" <> "ear" <> "_" <> "issue" <> "_number",
      "lin" <> "ear" <> "_" <> "comment" <> "_id",
      "git" <> "hub_pr_id",
      "git" <> "hub_pr_number",
      "git" <> "hub" <> "_" <> "issue" <> "_id",
      "git" <> "hub" <> "_" <> "issue" <> "_number",
      "co" <> "dex_session_id",
      "co" <> "dex_turn_id",
      "repo" <> "_full" <> "_name",
      "pull" <> "_number",
      "pull" <> "_request",
      "commit" <> "_sha"
    ]
  end
end
