defmodule MezzanineOpsModel.Intent.RunIntent do
  @moduledoc "Pure run-intent description derived from a work plan."

  alias MezzanineOpsModel.Builder

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
          intent_id: MezzanineOpsModel.id(),
          program_id: MezzanineOpsModel.id(),
          work_id: MezzanineOpsModel.id(),
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

defmodule MezzanineOpsModel.Intent.ReviewIntent do
  @moduledoc "Pure review-intent description derived from a work plan."

  alias MezzanineOpsModel.Builder

  @enforce_keys [:intent_id, :program_id, :work_id, :gate]
  defstruct [
    :intent_id,
    :program_id,
    :work_id,
    :gate,
    required_decisions: 1,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          intent_id: MezzanineOpsModel.id(),
          program_id: MezzanineOpsModel.id(),
          work_id: MezzanineOpsModel.id(),
          gate: atom() | String.t(),
          required_decisions: pos_integer(),
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end

defmodule MezzanineOpsModel.Intent.PlacementIntent do
  @moduledoc "Pure placement-intent description for run placement resolution."

  alias MezzanineOpsModel.Builder

  @enforce_keys [:intent_id, :placement_profile]
  defstruct [:intent_id, :placement_profile, target: nil, constraints: %{}, metadata: %{}]

  @type t :: %__MODULE__{
          intent_id: MezzanineOpsModel.id(),
          placement_profile: String.t(),
          target: String.t() | nil,
          constraints: map(),
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end

defmodule MezzanineOpsModel.Intent.EffectIntent do
  @moduledoc "Pure effect-intent description for future lower effects."

  alias MezzanineOpsModel.Builder

  @enforce_keys [:intent_id, :effect_type, :subject]
  defstruct [:intent_id, :effect_type, :subject, payload: %{}, metadata: %{}]

  @type t :: %__MODULE__{
          intent_id: MezzanineOpsModel.id(),
          effect_type: atom() | String.t(),
          subject: term(),
          payload: map(),
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end

defmodule MezzanineOpsModel.Intent.ReadIntent do
  @moduledoc "Pure read-intent description for future lower reads."

  alias MezzanineOpsModel.Builder

  @enforce_keys [:intent_id, :read_type, :subject]
  defstruct [:intent_id, :read_type, :subject, query: %{}, metadata: %{}]

  @type t :: %__MODULE__{
          intent_id: MezzanineOpsModel.id(),
          read_type: atom() | String.t(),
          subject: term(),
          query: map(),
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end

defmodule MezzanineOpsModel.Intent.NotificationIntent do
  @moduledoc "Pure notification-intent description for operator-facing notices."

  alias MezzanineOpsModel.Builder

  @enforce_keys [:intent_id, :channel, :audience]
  defstruct [:intent_id, :channel, :audience, template: nil, payload: %{}, metadata: %{}]

  @type t :: %__MODULE__{
          intent_id: MezzanineOpsModel.id(),
          channel: atom() | String.t(),
          audience: [String.t()] | String.t(),
          template: String.t() | nil,
          payload: map(),
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end
