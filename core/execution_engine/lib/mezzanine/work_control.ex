defmodule Mezzanine.WorkControl do
  @moduledoc """
  Neutral control-session reads and ensures for governed work.
  """

  require Ash.Query

  alias Mezzanine.Control.ControlSession
  alias Mezzanine.Work.WorkObject

  @type control_session_record :: struct()
  @type work_object_record :: struct()

  @spec control_session_for_work(String.t(), Ecto.UUID.t()) ::
          {:ok, control_session_record() | nil} | {:error, term()}
  def control_session_for_work(tenant_id, work_object_id)
      when is_binary(tenant_id) and is_binary(work_object_id) do
    ControlSession
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id == ^work_object_id)
    |> Ash.read(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Control)
    |> case do
      {:ok, [control_session | _]} -> {:ok, control_session}
      {:ok, []} -> {:ok, nil}
      {:error, error} -> {:error, error}
    end
  end

  @spec ensure_control_session(String.t(), work_object_record()) ::
          {:ok, control_session_record()} | {:error, term()}
  def ensure_control_session(tenant_id, %WorkObject{} = work_object)
      when is_binary(tenant_id) do
    case control_session_for_work(tenant_id, work_object.id) do
      {:ok, %ControlSession{} = control_session} ->
        {:ok, control_session}

      {:ok, nil} ->
        ControlSession.open(
          %{program_id: work_object.program_id, work_object_id: work_object.id},
          actor: actor(tenant_id),
          tenant: tenant_id
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec open_control_sessions(String.t(), Ecto.UUID.t()) ::
          {:ok, [control_session_record()]} | {:error, term()}
  def open_control_sessions(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    case ControlSession.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id) do
      {:ok, control_sessions} ->
        {:ok, Enum.filter(control_sessions, &(&1.status == :active))}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp actor(tenant_id), do: %{tenant_id: tenant_id}
end
