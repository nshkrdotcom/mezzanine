defmodule Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.ConnectionInstaller do
  @moduledoc false

  alias Jido.Integration.V2
  alias Jido.Integration.V2.Connectors.CodexCli
  alias Jido.Integration.V2.RuntimeRouter
  alias Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.Support

  @connector_id "codex_cli"
  @scopes ["session:execute", "session:control", "session:tools"]

  @spec maybe_start_runtime_router(keyword()) ::
          :ok | {:error, :codex_runtime_router_not_available}
  def maybe_start_runtime_router(opts) do
    if Keyword.get(opts, :start_runtime_router?, true) do
      if Code.ensure_loaded?(RuntimeRouter) and function_exported?(RuntimeRouter, :start!, 0) do
        RuntimeRouter.start!()
      else
        {:error, :codex_runtime_router_not_available}
      end
    else
      :ok
    end
  end

  @spec maybe_register_connector(keyword()) :: :ok | {:error, :codex_connector_not_available}
  def maybe_register_connector(opts) do
    if Keyword.get(opts, :register_connector?, true) do
      if Code.ensure_loaded?(CodexCli) and function_exported?(CodexCli, :manifest, 0) do
        V2.register_connector(CodexCli)
      else
        {:error, :codex_connector_not_available}
      end
    else
      :ok
    end
  end

  @spec connection_id(map(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def connection_id(attrs, opts) do
    case Keyword.get(opts, :connection_id) do
      connection_id when is_binary(connection_id) and connection_id != "" ->
        {:ok, connection_id}

      _missing ->
        install(attrs, opts)
    end
  end

  @spec install(map(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def install(attrs, opts) do
    start_install_fun = Keyword.get(opts, :start_install_fun, &V2.start_install/3)
    complete_install_fun = Keyword.get(opts, :complete_install_fun, &V2.complete_install/2)
    tenant_id = Keyword.get(opts, :tenant_id, Support.map_value(attrs, :tenant_ref))
    actor_id = Keyword.get(opts, :actor_id, Support.actor_id(attrs))
    subject = Keyword.get(opts, :subject, "codex-cli-native-auth")

    with {:ok, %{install: install, connection: connection}} <-
           start_install_fun.(@connector_id, tenant_id, %{
             actor_id: actor_id,
             auth_type: :api_token,
             profile_id: "native_codex_cli",
             subject: subject,
             requested_scopes: @scopes
           }),
         {:ok, %{connection: completed_connection}} <-
           complete_install_fun.(install.install_id, %{
             subject: subject,
             granted_scopes: @scopes,
             secret: %{access_token: "codex-native-auth-redacted"}
           }) do
      {:ok,
       Support.map_value(completed_connection, :connection_id) ||
         Support.map_value(connection, :connection_id)}
    end
  end
end
