defmodule Mezzanine.Core.GovernedEffects.TransitionGate do
  @moduledoc """
  Pure transition validation for governed-effect lifecycle states.
  """

  alias Mezzanine.Core.GovernedEffects.GovernedEffect
  alias Mezzanine.Core.GovernedEffects.Support

  @registered_effect_types [
    "diagnostic",
    "diagnostic.echo",
    "diagnostic.probe",
    "http.probe",
    "system.diagnostic",
    "workspace.health_check"
  ]

  @transitions %{
    proposed: [:authorized, :denied],
    authorized: [:dispatched, :denied],
    dispatched: [:receipt_received, :denied],
    receipt_received: [:reduced, :compensating],
    reduced: [:projected, :compensating],
    projected: [:completed, :compensating],
    compensating: [:completed],
    completed: [],
    denied: []
  }

  @type option ::
          {:authority_ref, String.t()}
          | {:expected_version, integer()}
          | {:registered_effect_types, [String.t() | atom()]}
          | {:updated_at, String.t()}

  @spec registered_effect_types() :: [String.t()]
  def registered_effect_types, do: @registered_effect_types

  @spec transition(GovernedEffect.t() | map() | keyword(), atom() | String.t(), [option()]) ::
          {:ok, GovernedEffect.t()} | {:error, term()}
  def transition(effect, next_status, opts \\ [])

  def transition(%GovernedEffect{} = effect, next_status, opts) do
    with {:ok, next_status} <-
           Support.bounded_atom(next_status, GovernedEffect.statuses(), :invalid_status),
         :ok <- validate_tenant(effect),
         :ok <- validate_effect_type(effect, opts),
         :ok <- validate_version(effect, opts),
         :ok <- validate_transition(effect.status, next_status),
         {:ok, authority_ref} <- validate_authority(effect, next_status, opts) do
      effect
      |> Map.from_struct()
      |> Map.merge(%{
        authority_ref: authority_ref,
        status: next_status,
        expected_version: next_version(effect, opts),
        updated_at: Keyword.get(opts, :updated_at, effect.updated_at)
      })
      |> GovernedEffect.new()
    end
  end

  def transition(attrs, next_status, opts) when is_map(attrs) or is_list(attrs) do
    with {:ok, effect} <- GovernedEffect.new(attrs) do
      transition(effect, next_status, opts)
    end
  end

  def transition(_effect, _next_status, _opts), do: {:error, :invalid_governed_effect}

  defp validate_tenant(%GovernedEffect{tenant_ref: tenant_ref, effect_ref: effect_ref})
       when is_binary(tenant_ref) and tenant_ref != "" do
    if is_binary(effect_ref) and effect_ref != "" do
      :ok
    else
      {:error, {:missing_effect_ref, effect_ref}}
    end
  end

  defp validate_tenant(%GovernedEffect{effect_ref: effect_ref}),
    do: {:error, {:missing_tenant_ref, effect_ref}}

  defp validate_effect_type(%GovernedEffect{effect_type: effect_type}, opts) do
    registered =
      opts |> Keyword.get(:registered_effect_types, @registered_effect_types) |> normalize_types()

    case normalize_type(effect_type) do
      {:ok, type} ->
        if MapSet.member?(registered, type) do
          :ok
        else
          {:error, {:unregistered_effect_type, type}}
        end

      :error ->
        {:error, {:unregistered_effect_type, effect_type}}
    end
  end

  defp validate_version(%GovernedEffect{} = effect, opts) do
    if Keyword.has_key?(opts, :expected_version) do
      expected = Keyword.fetch!(opts, :expected_version)
      actual = effect.expected_version

      if expected == actual do
        :ok
      else
        {:error, {:version_conflict, %{expected: expected, actual: actual}}}
      end
    else
      :ok
    end
  end

  defp validate_transition(current_status, next_status) do
    allowed = Map.get(@transitions, current_status, [])

    if next_status in allowed do
      :ok
    else
      {:error, {:invalid_transition, current_status, next_status}}
    end
  end

  defp validate_authority(effect, :authorized, opts) do
    authority_ref = Keyword.get(opts, :authority_ref) || effect.authority_ref

    if is_binary(authority_ref) and authority_ref != "" do
      {:ok, authority_ref}
    else
      {:error, {:missing_authority_ref, :authorized}}
    end
  end

  defp validate_authority(effect, _next_status, opts),
    do: {:ok, Keyword.get(opts, :authority_ref, effect.authority_ref)}

  defp next_version(%GovernedEffect{} = effect, opts) do
    cond do
      Keyword.has_key?(opts, :expected_version) and
          is_integer(Keyword.fetch!(opts, :expected_version)) ->
        Keyword.fetch!(opts, :expected_version) + 1

      is_integer(effect.expected_version) ->
        effect.expected_version + 1

      true ->
        effect.expected_version
    end
  end

  defp normalize_types(types), do: types |> Enum.map(&normalize_type!/1) |> MapSet.new()

  defp normalize_type!(type) do
    case normalize_type(type) do
      {:ok, normalized} -> normalized
      :error -> inspect(type)
    end
  end

  defp normalize_type(type) when is_binary(type) and type != "", do: {:ok, type}

  defp normalize_type(type) when is_atom(type) and not is_nil(type),
    do: {:ok, Atom.to_string(type)}

  defp normalize_type(_type), do: :error
end
