defmodule Mezzanine.Policy.FrontmatterParser do
  @moduledoc """
  Parses Markdown policy files with optional YAML front matter.
  """

  @spec parse(String.t()) ::
          {:ok, %{config: map(), prompt_template: String.t()}}
          | {:error, :workflow_front_matter_not_a_map | {:workflow_parse_error, term()}}
  def parse(content) when is_binary(content) do
    {front_matter_lines, prompt_lines} = split_front_matter(content)

    case decode_front_matter(front_matter_lines) do
      {:ok, front_matter} ->
        {:ok,
         %{
           config: front_matter,
           prompt_template: prompt_lines |> Enum.join("\n") |> String.trim()
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp split_front_matter(content) do
    lines =
      content
      |> String.replace("\r\n", "\n")
      |> String.replace("\r", "\n")
      |> String.split("\n", trim: false)

    case lines do
      ["---" | tail] ->
        {front, rest} = Enum.split_while(tail, &(&1 != "---"))

        case rest do
          ["---" | prompt_lines] -> {front, prompt_lines}
          _ -> {front, []}
        end

      _ ->
        {[], lines}
    end
  end

  defp decode_front_matter(lines) do
    yaml = Enum.join(lines, "\n")

    if String.trim(yaml) == "" do
      {:ok, %{}}
    else
      case YamlElixir.read_from_string(yaml) do
        {:ok, decoded} when is_map(decoded) -> {:ok, decoded}
        {:ok, _other} -> {:error, :workflow_front_matter_not_a_map}
        {:error, reason} -> {:error, {:workflow_parse_error, reason}}
      end
    end
  end
end
