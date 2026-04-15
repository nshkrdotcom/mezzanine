defmodule MezzanineOpsModel.WorkClass do
  @moduledoc "Reusable classification metadata for work objects."

  alias MezzanineOpsModel.Builder

  @enforce_keys [:class_id, :program_id, :name]
  defstruct [
    :class_id,
    :program_id,
    :name,
    description: nil,
    default_policy_ref: nil,
    default_placement_profile_id: nil,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          class_id: MezzanineOpsModel.id(),
          program_id: MezzanineOpsModel.id(),
          name: String.t(),
          description: String.t() | nil,
          default_policy_ref: String.t() | nil,
          default_placement_profile_id: String.t() | nil,
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end

defmodule MezzanineOpsModel.WorkObject do
  @moduledoc "Pure durable-work description before any Ash backing exists."

  alias MezzanineOpsModel.{Builder, Normalizer, WorkStatus}

  @enforce_keys [:work_id, :program_id, :work_type, :title, :payload, :status]
  defstruct [
    :work_id,
    :program_id,
    :work_type,
    :title,
    :payload,
    :status,
    normalized_payload: %{},
    policy_ref: nil,
    placement_profile_id: nil,
    dependency_keys: [],
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          work_id: MezzanineOpsModel.id(),
          program_id: MezzanineOpsModel.id(),
          work_type: String.t(),
          title: String.t(),
          payload: map(),
          normalized_payload: map(),
          status: WorkStatus.t(),
          policy_ref: String.t() | nil,
          placement_profile_id: String.t() | nil,
          dependency_keys: [String.t()],
          metadata: map()
        }

  def new(attrs) do
    {:ok, new!(attrs)}
  rescue
    error in [ArgumentError, KeyError] -> {:error, error}
  end

  def new!(attrs) do
    __MODULE__
    |> Builder.build!(normalize(attrs))
  end

  defp normalize(attrs) when is_map(attrs) do
    status = attrs |> Map.fetch!(:status) |> WorkStatus.cast!()
    payload = Map.fetch!(attrs, :payload)

    attrs
    |> Map.put(:status, status)
    |> Map.put_new(:normalized_payload, Normalizer.normalize_payload(payload))
    |> Map.update(:metadata, %{}, &Normalizer.normalize_payload/1)
    |> Map.update(:dependency_keys, [], &Enum.uniq/1)
  end
end

defmodule MezzanineOpsModel.WorkPlan do
  @moduledoc "Pure plan produced from a work object and compiled policy."

  alias MezzanineOpsModel.Builder

  @enforce_keys [:plan_id, :work_id]
  defstruct [
    :plan_id,
    :work_id,
    derived_run_intents: [],
    derived_review_intents: [],
    derived_effect_intents: [],
    derived_read_intents: [],
    derived_notification_intents: [],
    obligations: [],
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          plan_id: MezzanineOpsModel.id(),
          work_id: MezzanineOpsModel.id(),
          derived_run_intents: [MezzanineOpsModel.Intent.RunIntent.t()],
          derived_review_intents: [MezzanineOpsModel.Intent.ReviewIntent.t()],
          derived_effect_intents: [MezzanineOpsModel.Intent.EffectIntent.t()],
          derived_read_intents: [MezzanineOpsModel.Intent.ReadIntent.t()],
          derived_notification_intents: [MezzanineOpsModel.Intent.NotificationIntent.t()],
          obligations: [MezzanineOpsModel.Obligation.t()],
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end

defmodule MezzanineOpsModel.Run do
  @moduledoc "Pure run description independent of any lower runtime owner."

  alias MezzanineOpsModel.{Builder, RunStatus}

  @enforce_keys [:run_id, :work_id, :status, :intent]
  defstruct [
    :run_id,
    :work_id,
    :status,
    :intent,
    series_id: nil,
    attempts: 0,
    last_error: nil,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          run_id: MezzanineOpsModel.id(),
          work_id: MezzanineOpsModel.id(),
          status: RunStatus.t(),
          intent: MezzanineOpsModel.Intent.RunIntent.t(),
          series_id: MezzanineOpsModel.id() | nil,
          attempts: non_neg_integer(),
          last_error: term(),
          metadata: map()
        }

  def new(attrs) do
    {:ok, new!(attrs)}
  rescue
    error in [ArgumentError, KeyError] -> {:error, error}
  end

  def new!(attrs) do
    __MODULE__
    |> Builder.build!(normalize(attrs))
  end

  defp normalize(attrs) when is_map(attrs) do
    Map.update!(attrs, :status, &RunStatus.cast!/1)
  end
end

defmodule MezzanineOpsModel.Obligation do
  @moduledoc "Pure obligation derived from a work plan."

  alias MezzanineOpsModel.Builder

  @enforce_keys [:obligation_id, :work_id, :obligation_type, :state]
  defstruct [
    :obligation_id,
    :work_id,
    :obligation_type,
    :state,
    subject: nil,
    due_at: nil,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          obligation_id: MezzanineOpsModel.id(),
          work_id: MezzanineOpsModel.id(),
          obligation_type: atom() | String.t(),
          state: atom() | String.t(),
          subject: term(),
          due_at: DateTime.t() | nil,
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end

defmodule MezzanineOpsModel.ReviewUnit do
  @moduledoc "Pure review bundle awaiting or carrying human/system decisions."

  alias MezzanineOpsModel.{Builder, ReviewStatus}

  @enforce_keys [:review_id, :work_id, :status, :gate]
  defstruct [
    :review_id,
    :work_id,
    :status,
    :gate,
    required_decisions: 1,
    decisions: [],
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          review_id: MezzanineOpsModel.id(),
          work_id: MezzanineOpsModel.id(),
          status: ReviewStatus.t(),
          gate: atom() | String.t(),
          required_decisions: pos_integer(),
          decisions: [term()],
          metadata: map()
        }

  def new(attrs) do
    {:ok, new!(attrs)}
  rescue
    error in [ArgumentError, KeyError] -> {:error, error}
  end

  def new!(attrs) do
    __MODULE__
    |> Builder.build!(normalize(attrs))
  end

  defp normalize(attrs) when is_map(attrs) do
    Map.update!(attrs, :status, &ReviewStatus.cast!/1)
  end
end

defmodule MezzanineOpsModel.EvidenceBundle do
  @moduledoc "Pure evidence bundle collected around a work object or run."

  alias MezzanineOpsModel.Builder

  @enforce_keys [:bundle_id, :work_id]
  defstruct [:bundle_id, :work_id, items: [], summary: %{}, metadata: %{}]

  @type t :: %__MODULE__{
          bundle_id: MezzanineOpsModel.id(),
          work_id: MezzanineOpsModel.id(),
          items: [term()],
          summary: map(),
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end

defmodule MezzanineOpsModel.PolicyBundle do
  @moduledoc "Pure policy bundle with raw config, prompt template, and compiled form."

  alias MezzanineOpsModel.Builder

  @enforce_keys [:bundle_id, :source_ref, :config, :prompt_template]
  defstruct [
    :bundle_id,
    :source_ref,
    :config,
    :prompt_template,
    compiled_form: %{},
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          bundle_id: MezzanineOpsModel.id(),
          source_ref: String.t(),
          config: map(),
          prompt_template: String.t(),
          compiled_form: map(),
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end

defmodule MezzanineOpsModel.CapabilityGrant do
  @moduledoc "Pure capability-grant declaration derived from policy."

  alias MezzanineOpsModel.Builder

  @enforce_keys [:capability_id, :mode]
  defstruct [:capability_id, :mode, scope: nil, constraints: %{}]

  @type t :: %__MODULE__{
          capability_id: String.t(),
          mode: :allow | :deny | :escalate,
          scope: String.t() | nil,
          constraints: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end

defmodule MezzanineOpsModel.PlacementProfile do
  @moduledoc "Pure placement profile derived from policy or authored defaults."

  alias MezzanineOpsModel.Builder

  @enforce_keys [:profile_id, :strategy]
  defstruct [
    :profile_id,
    :strategy,
    target_selector: %{},
    runtime_preferences: %{},
    workspace_policy: %{},
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          profile_id: String.t(),
          strategy: atom() | String.t(),
          target_selector: map(),
          runtime_preferences: map(),
          workspace_policy: map(),
          metadata: map()
        }

  def new(attrs), do: Builder.build(__MODULE__, attrs)
  def new!(attrs), do: Builder.build!(__MODULE__, attrs)
end
