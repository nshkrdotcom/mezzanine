defmodule Mezzanine.AgentRuntime.ProfileSlotRef do
  @moduledoc "Typed profile slot reference used by S0 profile bundles."

  @slots [
    :source_profile_ref,
    :runtime_profile_ref,
    :tool_scope_ref,
    :evidence_profile_ref,
    :publication_profile_ref,
    :review_profile_ref,
    :memory_profile_ref,
    :projection_profile_ref
  ]

  @enforce_keys [:slot, :ref]
  defstruct [:slot, :ref]

  @type ref :: atom() | {:custom, String.t()}
  @type t :: %__MODULE__{slot: atom(), ref: ref()}

  def slots, do: @slots

  def new(%__MODULE__{} = ref), do: {:ok, ref}

  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(%{} = attrs) do
    slot = Map.get(attrs, :slot, Map.get(attrs, "slot"))
    ref = Map.get(attrs, :ref, Map.get(attrs, "ref"))

    if slot in @slots and valid_ref?(slot, ref) do
      {:ok, %__MODULE__{slot: slot, ref: ref}}
    else
      {:error, :invalid_profile_slot_ref}
    end
  end

  def new(_attrs), do: {:error, :invalid_profile_slot_ref}

  def new!(attrs) do
    case new(attrs) do
      {:ok, ref} -> ref
      {:error, reason} -> raise ArgumentError, Atom.to_string(reason)
    end
  end

  def dump(%__MODULE__{slot: slot, ref: {:custom, ref}}),
    do: %{"slot" => Atom.to_string(slot), "ref" => %{"custom" => ref}}

  def dump(%__MODULE__{slot: slot, ref: ref}),
    do: %{"slot" => Atom.to_string(slot), "ref" => Atom.to_string(ref)}

  def valid_ref?(:memory_profile_ref, :none), do: true
  def valid_ref?(:memory_profile_ref, :private_facts_v1), do: true
  def valid_ref?(_slot, {:custom, value}), do: is_binary(value) and String.trim(value) != ""
  def valid_ref?(_slot, value), do: is_atom(value) and not is_nil(value)
end

defmodule Mezzanine.AgentRuntime.ProfileBundle do
  @moduledoc "Complete S0 profile slot bundle."

  alias Mezzanine.AgentRuntime.ProfileSlotRef

  @slots ProfileSlotRef.slots()
  @enforce_keys @slots
  defstruct @slots

  @type t :: %__MODULE__{}

  def new(%__MODULE__{} = bundle), do: {:ok, bundle}
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(%{} = attrs) do
    with :ok <- reject_unknown(attrs),
         {:ok, values} <- collect(attrs) do
      {:ok, struct!(__MODULE__, values)}
    end
  end

  def new(_attrs), do: {:error, :invalid_profile_bundle}

  def new!(attrs) do
    case new(attrs) do
      {:ok, bundle} -> bundle
      {:error, reason} -> raise ArgumentError, Atom.to_string(reason)
    end
  end

  def dump(%__MODULE__{} = bundle) do
    Map.new(@slots, fn slot ->
      ref = Map.fetch!(Map.from_struct(bundle), slot)
      {Atom.to_string(slot), ProfileSlotRef.dump(%ProfileSlotRef{slot: slot, ref: ref})["ref"]}
    end)
  end

  defp collect(attrs) do
    Enum.reduce_while(@slots, {:ok, %{}}, fn slot, {:ok, acc} ->
      ref = Map.get(attrs, slot, Map.get(attrs, Atom.to_string(slot)))

      if ProfileSlotRef.valid_ref?(slot, ref) do
        {:cont, {:ok, Map.put(acc, slot, ref)}}
      else
        {:halt, {:error, :invalid_profile_bundle}}
      end
    end)
  end

  defp reject_unknown(attrs) do
    allowed = MapSet.new(Enum.flat_map(@slots, &[&1, Atom.to_string(&1)]))

    if Enum.all?(Map.keys(attrs), &MapSet.member?(allowed, &1)) do
      :ok
    else
      {:error, :invalid_profile_bundle}
    end
  end
end

defmodule Mezzanine.AgentRuntime.ProfileRegistry do
  @moduledoc "Profile slot registry populated from compiled packs."

  alias Mezzanine.AgentRuntime.{ProfileBundle, ProfileSlotRef}

  defstruct entries: %{}

  def new(entries \\ []) when is_list(entries) do
    Enum.reduce_while(entries, {:ok, %__MODULE__{}}, fn entry, {:ok, registry} ->
      case register(registry, entry) do
        {:ok, registry} -> {:cont, {:ok, registry}}
        error -> {:halt, error}
      end
    end)
  end

  def register(%__MODULE__{} = registry, attrs) when is_map(attrs) do
    slot = Map.get(attrs, :slot, Map.get(attrs, "slot"))
    ref = Map.get(attrs, :ref, Map.get(attrs, "ref"))
    module = Map.get(attrs, :module, Map.get(attrs, "module"))

    if slot in ProfileSlotRef.slots() and ProfileSlotRef.valid_ref?(slot, ref) and is_atom(module) do
      {:ok, put_in(registry.entries[{slot, ref}], Map.new(attrs))}
    else
      {:error, :invalid_profile_registry_entry}
    end
  end

  def lookup(%__MODULE__{} = registry, slot, ref), do: Map.fetch(registry.entries, {slot, ref})

  def validate_bundle(%__MODULE__{} = registry, %ProfileBundle{} = bundle) do
    bundle
    |> Map.from_struct()
    |> Enum.reduce_while(:ok, fn {slot, ref}, :ok ->
      case lookup(registry, slot, ref) do
        {:ok, _entry} -> {:cont, :ok}
        :error -> {:halt, {:error, {:unregistered_profile_slot, slot, ref}}}
      end
    end)
  end

  def dump(%__MODULE__{} = registry) do
    registry.entries
    |> Map.values()
    |> Enum.map(fn entry ->
      Map.new(entry, fn {key, value} -> {to_string(key), dump_value(value)} end)
    end)
    |> Enum.sort_by(&{&1["slot"], inspect(&1["ref"])})
  end

  defp dump_value(value) when is_atom(value), do: Atom.to_string(value)
  defp dump_value(value), do: value
end
