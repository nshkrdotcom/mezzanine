defmodule Mezzanine.RemoteFacade.Workflow do
  @moduledoc """
  Mezzanine-owned workflow facade for distributed StackLab profiles.

  This facade is intentionally small: AppKit submits governed work envelopes
  and receives an accepted ref; readback returns bounded workflow projection
  facts. Durable execution internals remain below Mezzanine owner packages.
  """

  @owner_group {__MODULE__, :workflow}
  @required_fields ~w(
    schema_ref
    tenant_ref
    correlation_ref
    idempotency_key
    trace_ref
    authority_ref
    payload_mode
    redaction_class
  )

  @spec owner_group() :: {module(), :workflow}
  def owner_group, do: @owner_group

  @spec submit_work(map(), keyword()) :: {:ok, map()} | {:error, map()}
  def submit_work(request, opts \\ []) when is_map(request) and is_list(opts) do
    with :ok <- validate_envelope(request),
         :ok <- validate_payload_mode(request) do
      accepted_ref = accepted_ref(request)

      {:ok,
       %{
         "status" => "accepted",
         "accepted_ref" => accepted_ref,
         "correlation_ref" => string_value(request, "correlation_ref"),
         "idempotency_key" => string_value(request, "idempotency_key"),
         "tenant_ref" => string_value(request, "tenant_ref"),
         "trace_ref" => string_value(request, "trace_ref"),
         "readback_ref" => accepted_ref <> "/readback",
         "async_contract" => "accepted_ref_plus_readback"
       }}
    end
  end

  @spec readback(String.t(), keyword()) :: {:ok, map()} | {:error, map()}
  def readback(ref, opts \\ []) when is_binary(ref) and is_list(opts) do
    if String.trim(ref) == "" do
      {:error, error(:invalid_envelope, %{"missing_field" => "ref"})}
    else
      {:ok,
       %{
         "status" => Keyword.get(opts, :status, "accepted"),
         "accepted_ref" => ref,
         "projection_ref" => ref <> "/projection",
         "terminal?" => Keyword.get(opts, :terminal?, false),
         "owner" => "mezzanine"
       }}
    end
  end

  defp validate_envelope(request) do
    case Enum.find(@required_fields, &(string_value(request, &1) == nil)) do
      nil -> :ok
      field -> {:error, error(:invalid_envelope, %{"missing_field" => field})}
    end
  end

  defp validate_payload_mode(request) do
    case string_value(request, "payload_mode") do
      mode when mode in ["refs_only", "bounded_summary", "claim_check"] ->
        :ok

      _other ->
        {:error, error(:payload_not_allowed)}
    end
  end

  defp accepted_ref(request) do
    "mezzanine-work://#{URI.encode_www_form(string_value(request, "idempotency_key"))}"
  end

  defp string_value(map, field) do
    value = Map.get(map, field) || Map.get(map, String.to_atom(field))

    if is_binary(value) and String.trim(value) != "" do
      value
    end
  end

  defp error(code, attrs \\ %{}) do
    Map.merge(
      %{
        "code" => Atom.to_string(code),
        "owner" => "mezzanine",
        "facade" => "workflow"
      },
      attrs
    )
  end
end
