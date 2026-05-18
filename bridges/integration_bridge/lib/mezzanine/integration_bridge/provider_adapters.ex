defmodule Mezzanine.IntegrationBridge.ProviderAdapters do
  @moduledoc """
  Adapter-zone resolver for explicit product binding refs.

  Generic dispatchers call this only after a binding has selected an adapter ref.
  The bridge keeps concrete provider modules here so product runtime code can
  carry provider facts as data without importing lower-boundary modules.
  """

  alias Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.AgentRuntime
  alias Mezzanine.IntegrationBridge.ProviderAdapters.GitHub.PrBranchCleanupRuntime
  alias Mezzanine.IntegrationBridge.ProviderAdapters.GitHub.PrEvidenceRuntime
  alias Mezzanine.IntegrationBridge.ProviderAdapters.Linear.GraphQLToolExecutor
  alias Mezzanine.IntegrationBridge.ProviderAdapters.Linear.SourceDispatcher

  @spec resolve(term(), atom()) :: {:ok, module()} | {:error, term()}
  def resolve(adapter_ref, adapter_kind) when is_atom(adapter_kind) do
    adapter_ref
    |> normalize_ref()
    |> do_resolve(adapter_kind)
  end

  defp normalize_ref(ref) when is_atom(ref), do: ref |> Atom.to_string() |> normalize_ref()

  defp normalize_ref(ref) when is_binary(ref) do
    ref
    |> String.split(["/", ":", "@", "."], trim: true)
    |> List.last()
  end

  defp normalize_ref(_ref), do: nil

  defp do_resolve("codex_cli", :runtime), do: {:ok, AgentRuntime}
  defp do_resolve("linear", :source), do: {:ok, SourceDispatcher}
  defp do_resolve("linear", :tool), do: {:ok, GraphQLToolExecutor}
  defp do_resolve("github", :evidence), do: {:ok, PrEvidenceRuntime}
  defp do_resolve("github", :resource_effect), do: {:ok, PrBranchCleanupRuntime}
  defp do_resolve(ref, kind), do: {:error, {:unsupported_provider_adapter, kind, ref}}
end
