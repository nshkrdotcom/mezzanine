defmodule Mezzanine.RuntimeProfileStore do
  @moduledoc """
  Supervised owner for the boot-loaded Mezzanine runtime profile.

  Production code reads profile values from this owner instead of consulting
  application env deep in call stacks. Tests may replace the profile through
  the explicit API and restore the previous value in `on_exit/1`.
  """

  use GenServer

  alias Mezzanine.RuntimeProfile

  @type server :: GenServer.server()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) when is_list(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    profile = Keyword.get_lazy(opts, :profile, &RuntimeProfile.from_env/0)
    genserver_opts = if is_nil(name), do: [], else: [name: name]

    GenServer.start_link(__MODULE__, profile, genserver_opts)
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :id, __MODULE__),
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @doc "Returns the current runtime profile, or an empty profile if no owner is running."
  @spec profile(server()) :: RuntimeProfile.t()
  def profile(server \\ __MODULE__) do
    case running?(server) do
      true -> GenServer.call(server, :profile)
      false -> RuntimeProfile.empty()
    end
  end

  @doc "Replaces the current runtime profile and returns the previous profile."
  @spec replace_profile(RuntimeProfile.t(), server()) ::
          {:ok, RuntimeProfile.t()} | {:error, :not_running}
  def replace_profile(%RuntimeProfile{} = profile, server \\ __MODULE__) do
    case running?(server) do
      true -> GenServer.call(server, {:replace_profile, profile})
      false -> {:error, :not_running}
    end
  end

  @doc "Fetches a configured value from the current profile."
  @spec config(atom(), atom(), term(), server()) :: term()
  def config(app, key, default \\ nil, server \\ __MODULE__) do
    server
    |> profile()
    |> RuntimeProfile.config(app, key, default)
  end

  @doc "Fetches a configured keyword list from the current profile."
  @spec keyword_config(atom(), atom(), keyword(), server()) :: keyword()
  def keyword_config(app, key, default \\ [], server \\ __MODULE__) when is_list(default) do
    server
    |> profile()
    |> RuntimeProfile.keyword_config(app, key, default)
  end

  @doc "Fetches a configured module from the current profile."
  @spec module(atom(), atom(), module(), server()) :: module()
  def module(app, key, default, server \\ __MODULE__) do
    server
    |> profile()
    |> RuntimeProfile.module(app, key, default)
  end

  @impl true
  def init(%RuntimeProfile{} = profile), do: {:ok, profile}

  @impl true
  def handle_call(:profile, _from, profile), do: {:reply, profile, profile}

  def handle_call({:replace_profile, %RuntimeProfile{} = new_profile}, _from, profile) do
    {:reply, {:ok, profile}, new_profile}
  end

  defp running?(pid) when is_pid(pid), do: Process.alive?(pid)

  defp running?(name) when is_atom(name) do
    case Process.whereis(name) do
      nil -> false
      pid -> Process.alive?(pid)
    end
  end

  defp running?({:global, term}) do
    case :global.whereis_name(term) do
      :undefined -> false
      pid -> Process.alive?(pid)
    end
  end

  defp running?({:via, module, term}) do
    case module.whereis_name(term) do
      :undefined -> false
      nil -> false
      pid -> Process.alive?(pid)
    end
  end
end
