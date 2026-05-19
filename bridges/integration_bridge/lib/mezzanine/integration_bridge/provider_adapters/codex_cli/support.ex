defmodule Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.Support do
  @moduledoc false

  @connector_id "codex_cli"
  @codex_workspace_root "/tmp/jido_codex_cli_workspace"

  @spec codex_workspace_root(keyword()) :: String.t()
  def codex_workspace_root(opts) do
    sandbox_file_scope =
      opts
      |> Keyword.get(:sandbox, %{})
      |> map_value(:file_scope)

    Keyword.get(opts, :workspace_root) || Keyword.get(opts, :cwd) || sandbox_file_scope ||
      @codex_workspace_root
  end

  @spec codex_connector_manifest_ref() :: String.t()
  def codex_connector_manifest_ref, do: "manifest://jido/connectors/#{@connector_id}@local"

  @spec codex_connector_ref() :: String.t()
  def codex_connector_ref, do: "jido/connectors/#{@connector_id}"

  @spec capability_negotiation_ref(String.t() | nil) :: String.t() | nil
  def capability_negotiation_ref(lower_request_ref) when is_binary(lower_request_ref),
    do: "cap-neg://#{lower_request_ref}"

  def capability_negotiation_ref(_lower_request_ref), do: nil

  @spec codex_evidence_profile_ref(term()) :: String.t()
  def codex_evidence_profile_ref(run_ref),
    do: "evidence://codex-agent-runtime/#{ref_suffix(run_ref)}"

  @spec action_receipt_status_token(term()) :: String.t()
  def action_receipt_status_token("completed"), do: "succeeded"
  def action_receipt_status_token("stopped"), do: "succeeded"
  def action_receipt_status_token(status) when is_binary(status), do: status
  def action_receipt_status_token(status) when is_atom(status), do: Atom.to_string(status)
  def action_receipt_status_token(_status), do: "failed"

  @spec digest(term()) :: String.t()
  def digest(value) do
    value
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  @spec prompt_hash(String.t()) :: String.t()
  def prompt_hash(prompt) when is_binary(prompt) do
    digest = :crypto.hash(:sha256, prompt)
    "sha256:" <> Base.encode16(digest, case: :lower)
  end

  @spec normalize(map() | keyword() | struct()) :: map()
  def normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize()
  def normalize(attrs) when is_map(attrs), do: attrs
  def normalize(attrs) when is_list(attrs), do: Map.new(attrs)

  @spec actor_id(map()) :: term()
  def actor_id(attrs),
    do: map_value(attrs, :source_ref) || "actor://mezzanine/codex-agent-runtime"

  @spec requested_max_turns(map()) :: pos_integer()
  def requested_max_turns(attrs) do
    case map_value(attrs, :max_turns) do
      value when is_integer(value) and value > 0 ->
        value

      value when is_binary(value) ->
        case Integer.parse(value) do
          {parsed, ""} when parsed > 0 -> parsed
          _other -> 1
        end

      _other ->
        1
    end
  end

  @spec put_present(keyword() | map(), atom(), term()) :: keyword() | map()
  def put_present(container, _key, nil), do: container
  def put_present(keyword, key, value) when is_list(keyword), do: Keyword.put(keyword, key, value)
  def put_present(map, key, value) when is_map(map), do: Map.put(map, key, value)

  @spec put_map_present(map(), atom(), term()) :: map()
  def put_map_present(map, _key, nil), do: map
  def put_map_present(map, key, value), do: Map.put(map, key, value)

  @spec compact_map(map()) :: map()
  def compact_map(map) do
    map
    |> Enum.reject(fn {_key, value} -> value in [nil, "", [], %{}] end)
    |> Map.new()
  end

  @spec compact_list(list()) :: list()
  def compact_list(list), do: Enum.reject(list, &is_nil/1)

  @spec map_value(term(), atom() | String.t()) :: term()
  def map_value(%_{} = struct, key), do: struct |> Map.from_struct() |> map_value(key)

  def map_value(%{} = map, key) when is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, nil} -> Map.get(map, Atom.to_string(key))
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  def map_value(%{} = map, key) when is_binary(key), do: Map.get(map, key)
  def map_value(_value, _key), do: nil

  @spec present_binary?(term()) :: boolean()
  def present_binary?(value), do: is_binary(value) and String.trim(value) != ""

  @spec non_empty(term()) :: String.t() | nil
  def non_empty(value) when is_binary(value) do
    if String.trim(value) == "", do: nil, else: value
  end

  def non_empty(_value), do: nil

  @spec truthy?(term()) :: boolean()
  def truthy?(value), do: value in [true, "true", "1", 1]

  @spec ref_suffix(term()) :: String.t()
  def ref_suffix(ref) when is_binary(ref) do
    ref
    |> :binary.bin_to_list()
    |> Enum.reduce({[], false}, &ascii_alnum_dash_byte/2)
    |> elem(0)
    |> Enum.reverse()
    |> List.to_string()
    |> String.trim("-")
  end

  def ref_suffix(ref), do: ref |> to_string() |> ref_suffix()

  defp ascii_alnum_dash_byte(byte, {chars, _previous_dash?}) when byte in ?A..?Z,
    do: {[byte | chars], false}

  defp ascii_alnum_dash_byte(byte, {chars, _previous_dash?}) when byte in ?a..?z,
    do: {[byte | chars], false}

  defp ascii_alnum_dash_byte(byte, {chars, _previous_dash?}) when byte in ?0..?9,
    do: {[byte | chars], false}

  defp ascii_alnum_dash_byte(_byte, {chars, true}), do: {chars, true}
  defp ascii_alnum_dash_byte(_byte, {chars, false}), do: {[?- | chars], true}
end
