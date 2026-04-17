defmodule Mezzanine.Intent do
  @moduledoc """
  Pure neutral intent structs shared by the active lower bridges.
  """

  @typedoc "Opaque identifier used across the neutral lower-intent seam."
  @type id :: String.t()
end

defmodule Mezzanine.Intent.Builder do
  @moduledoc false

  @spec build(module(), map() | keyword()) :: {:ok, struct()} | {:error, Exception.t()}
  def build(module, attrs) do
    {:ok, build!(module, attrs)}
  rescue
    error in [ArgumentError, KeyError] -> {:error, error}
  end

  @spec build!(module(), map() | keyword()) :: struct()
  def build!(module, attrs) when is_list(attrs) do
    attrs
    |> Enum.into(%{})
    |> build!(module)
  end

  def build!(module, attrs) when is_map(attrs) do
    struct!(module, attrs)
  end
end

defmodule Mezzanine.Intent.RunIntent do
  @moduledoc """
  Pure neutral run-intent description consumed by the active lower bridges.
  """

  alias Mezzanine.Intent.Builder

  @enforce_keys [:intent_id, :program_id, :work_id, :capability]
  defstruct [
    :intent_id,
    :program_id,
    :work_id,
    :capability,
    runtime_class: :session,
    placement: %{},
    grant_profile: %{},
    input: %{},
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          intent_id: Mezzanine.Intent.id(),
          program_id: Mezzanine.Intent.id(),
          work_id: Mezzanine.Intent.id(),
          capability: String.t(),
          runtime_class: atom(),
          placement: map(),
          grant_profile: map(),
          input: map(),
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end

defmodule Mezzanine.Intent.EffectIntent do
  @moduledoc """
  Pure neutral effect-intent description for lower effect dispatch.
  """

  alias Mezzanine.Intent.Builder

  @enforce_keys [:intent_id, :effect_type, :subject]
  defstruct [:intent_id, :effect_type, :subject, payload: %{}, metadata: %{}]

  @type t :: %__MODULE__{
          intent_id: Mezzanine.Intent.id(),
          effect_type: atom() | String.t(),
          subject: term(),
          payload: map(),
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end

defmodule Mezzanine.Intent.ReadIntent do
  @moduledoc """
  Pure neutral read-intent description for generic lower readback.
  """

  alias Mezzanine.Intent.Builder

  @enforce_keys [:intent_id, :read_type, :subject]
  defstruct [:intent_id, :read_type, :subject, query: %{}, metadata: %{}]

  @type t :: %__MODULE__{
          intent_id: Mezzanine.Intent.id(),
          read_type: atom() | String.t(),
          subject: term(),
          query: map(),
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end
