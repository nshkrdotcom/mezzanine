defmodule Mix.Tasks.Mezzanine.Workflow.Dispatch do
  use Mix.Task

  @moduledoc "Dispatches Mezzanine workflows backed by lower-plane bridge modules."
  @shortdoc "Dispatch a Mezzanine workflow"

  @impl true
  def run(["chassis_materialize_deployment" | args]) do
    Mix.Task.run("app.start")

    args
    |> attrs()
    |> Mezzanine.Workflow.ChassisDeploymentWorkflow.dispatch()
    |> case do
      {:ok, result} ->
        Mix.shell().info(
          "workflow=chassis_materialize_deployment status=#{result.status} receipt_ref=#{result.deployment_receipt_ref} app_ref=#{result.app_ref} outbox_delivered=#{result.outbox_delivered}"
        )

      {:error, reason} ->
        Mix.raise("chassis_materialize_deployment failed: #{inspect(reason)}")
    end
  end

  def run(["chassis_rollback_deployment" | args]) do
    Mix.Task.run("app.start")
    attrs = attrs(args)
    app_ref = Map.get(attrs, :app_ref) || Mix.raise("--app-ref is required")

    attrs
    |> Map.put(:app_ref, app_ref)
    |> Mezzanine.Workflow.ChassisRollbackWorkflow.dispatch()
    |> case do
      {:ok, result} ->
        Mix.shell().info(
          "workflow=chassis_rollback_deployment status=#{result.status} rollback_receipt_ref=#{result.rollback_receipt_ref} app_ref=#{result.app_ref}"
        )

      {:error, reason} ->
        Mix.raise("chassis_rollback_deployment failed: #{inspect(reason)}")
    end
  end

  def run([workflow | _args]) do
    Mix.raise("unsupported Mezzanine workflow #{inspect(workflow)}")
  end

  def run([]), do: Mix.raise("workflow name is required")

  defp attrs(args), do: parse_args(args, %{})

  defp parse_args(["--tenant-ref", value | rest], acc),
    do: parse_args(rest, Map.put(acc, :tenant_ref, value))

  defp parse_args(["--installation-ref", value | rest], acc),
    do: parse_args(rest, Map.put(acc, :installation_ref, value))

  defp parse_args(["--app-ref", value | rest], acc),
    do: parse_args(rest, Map.put(acc, :app_ref, value))

  defp parse_args(["--app-atom", value | rest], acc),
    do: parse_args(rest, Map.put(acc, :app_atom, existing_atom!(value)))

  defp parse_args(["--runtime-profile-ref", value | rest], acc),
    do: parse_args(rest, Map.put(acc, :runtime_profile_ref, value))

  defp parse_args(["--authority-ref", value | rest], acc),
    do: parse_args(rest, Map.put(acc, :authority_ref, value))

  defp parse_args(["--git-sha", value | rest], acc),
    do: parse_args(rest, Map.put(acc, :git_sha, value))

  defp parse_args(["--release-version", value | rest], acc),
    do: parse_args(rest, Map.put(acc, :release_version, value))

  defp parse_args([unknown | _rest], _acc), do: Mix.raise("unsupported option #{unknown}")
  defp parse_args([], acc), do: acc

  defp existing_atom!(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> Mix.raise("unsupported app atom #{inspect(value)}")
  end
end
