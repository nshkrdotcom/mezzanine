defmodule Mezzanine.Pack.Manifest do
  @moduledoc """
  Atomic pack definition returned from the `manifest/0` callback on `Mezzanine.Pack`.
  """

  alias Mezzanine.Pack.{
    ContextSourceSpec,
    DecisionSpec,
    EvidenceSpec,
    ExecutionRecipeSpec,
    LifecycleSpec,
    OperatorActionSpec,
    ProjectionSpec,
    SourceKindSpec,
    SubjectKindSpec
  }

  @type pack_identifier :: atom() | String.t()
  @type migration_strategy :: :additive | :force

  @type t :: %__MODULE__{
          pack_slug: pack_identifier(),
          version: String.t(),
          description: String.t() | nil,
          migration_strategy: migration_strategy(),
          max_supersession_depth: pos_integer(),
          subject_kind_specs: [SubjectKindSpec.t()],
          source_kind_specs: [SourceKindSpec.t()],
          context_source_specs: [ContextSourceSpec.t()],
          lifecycle_specs: [LifecycleSpec.t()],
          execution_recipe_specs: [ExecutionRecipeSpec.t()],
          decision_specs: [DecisionSpec.t()],
          evidence_specs: [EvidenceSpec.t()],
          operator_action_specs: [OperatorActionSpec.t()],
          projection_specs: [ProjectionSpec.t()]
        }

  defstruct [
    :pack_slug,
    :version,
    :description,
    migration_strategy: :additive,
    max_supersession_depth: 8,
    subject_kind_specs: [],
    source_kind_specs: [],
    context_source_specs: [],
    lifecycle_specs: [],
    execution_recipe_specs: [],
    decision_specs: [],
    evidence_specs: [],
    operator_action_specs: [],
    projection_specs: []
  ]
end

defmodule Mezzanine.Pack.SubjectKindSpec do
  @moduledoc """
  Subject-kind definition governed by a domain pack.
  """

  @type pack_identifier :: atom() | String.t()
  @type payload_field_type :: :string | :integer | :boolean | :map | :list
  @type payload_schema :: %{optional(atom()) => payload_field_type()}

  @type t :: %__MODULE__{
          name: pack_identifier(),
          description: String.t() | nil,
          payload_schema: payload_schema(),
          normalizer_mod: module() | nil
        }

  defstruct [:name, :description, payload_schema: %{}, normalizer_mod: nil]
end

defmodule Mezzanine.Pack.SourceKindSpec do
  @moduledoc """
  External source mapping into a normalized subject kind.
  """

  @type pack_identifier :: atom() | String.t()

  @type t :: %__MODULE__{
          name: pack_identifier(),
          subject_kind: pack_identifier(),
          description: String.t() | nil,
          adapter_mod: module() | nil
        }

  defstruct [:name, :subject_kind, :description, adapter_mod: nil]
end

defmodule Mezzanine.Pack.ContextSourceSpec do
  @moduledoc """
  Read-only context source bound into `outer_brain` context-pack assembly.
  """

  @type pack_identifier :: atom() | String.t()
  @type usage_phase :: :preprocess | :retrieval | :repair
  @type merge_strategy :: :append | :ranked_append | :replace_slot

  @type t :: %__MODULE__{
          source_ref: pack_identifier(),
          description: String.t() | nil,
          binding_key: pack_identifier(),
          usage_phase: usage_phase(),
          required?: boolean(),
          timeout_ms: pos_integer(),
          schema_ref: String.t() | nil,
          max_fragments: pos_integer(),
          merge_strategy: merge_strategy()
        }

  defstruct [
    :source_ref,
    :description,
    :binding_key,
    :usage_phase,
    :schema_ref,
    required?: false,
    timeout_ms: 1_000,
    max_fragments: 5,
    merge_strategy: :append
  ]
end

defmodule Mezzanine.Pack.LifecycleSpec do
  @moduledoc """
  State-machine definition for a single subject kind.
  """

  @type pack_identifier :: atom() | String.t()
  @type state :: pack_identifier()
  @type guard_ref :: %{required(:module) => module(), required(:function) => atom()}

  @type trigger ::
          :auto
          | {:execution_requested, recipe_ref :: pack_identifier()}
          | {:execution_completed, recipe_ref :: pack_identifier()}
          | {:execution_failed, recipe_ref :: pack_identifier()}
          | {:execution_failed, recipe_ref :: pack_identifier(), failure_kind :: atom()}
          | {:join_completed, join_step_ref :: pack_identifier()}
          | {:decision_made, decision_kind :: pack_identifier(), decision_value :: atom()}
          | {:operator_action, action_kind :: pack_identifier()}
          | {:subject_entered_state, state()}

  @type transition :: %{
          required(:from) => state(),
          required(:to) => state(),
          required(:trigger) => trigger(),
          optional(:guard) => guard_ref()
        }

  @type t :: %__MODULE__{
          subject_kind: pack_identifier(),
          initial_state: state(),
          terminal_states: [state()],
          transitions: [transition()]
        }

  defstruct [:subject_kind, :initial_state, terminal_states: [], transitions: []]
end

defmodule Mezzanine.Pack.ExecutionRecipeSpec do
  @moduledoc """
  Execution recipe definition referenced by lifecycle transitions.
  """

  @type pack_identifier :: atom() | String.t()
  @type lifecycle_hint_ref :: pack_identifier()
  @type runtime_class :: :session | :workflow | :playbook | :scan | :inference

  @type execution_failure_kind ::
          :transient_failure
          | :timeout
          | :infrastructure_error
          | :auth_error
          | :semantic_failure
          | :fatal_error

  @type retry_config :: %{
          required(:max_attempts) => pos_integer(),
          required(:backoff) => :linear | :exponential,
          optional(:retry_on) => [execution_failure_kind()],
          optional(:rekey_on) => [execution_failure_kind()]
        }

  @type workspace_policy :: %{
          required(:strategy) => :per_subject | :per_execution | :shared | :none,
          optional(:reuse) => boolean(),
          optional(:cleanup) => :on_completion | :on_terminal | :never
        }

  @type grant_spec :: %{
          optional(atom()) => %{
            optional(:allowed) => boolean(),
            optional(:scope) => atom() | String.t()
          }
        }

  @type t :: %__MODULE__{
          recipe_ref: pack_identifier(),
          description: String.t() | nil,
          runtime_class: runtime_class(),
          placement_ref: pack_identifier(),
          required_lifecycle_hints: [lifecycle_hint_ref()],
          grant_spec: grant_spec(),
          retry_config: retry_config(),
          workspace_policy: workspace_policy(),
          execution_params: map(),
          applicable_to: [pack_identifier()]
        }

  defstruct [
    :recipe_ref,
    :description,
    :runtime_class,
    :placement_ref,
    required_lifecycle_hints: [],
    grant_spec: %{},
    retry_config: %{max_attempts: 3, backoff: :exponential},
    workspace_policy: %{strategy: :per_subject},
    execution_params: %{},
    applicable_to: []
  ]
end

defmodule Mezzanine.Pack.DecisionSpec do
  @moduledoc """
  Decision-gate definition for a neutral subject workflow.
  """

  @type pack_identifier :: atom() | String.t()

  @type decision_trigger ::
          {:after_execution_completed, recipe_ref :: pack_identifier()}
          | {:after_decision, prior_kind :: pack_identifier(), decision_value :: atom()}
          | {:on_subject_entered_state, state :: pack_identifier()}

  @type decision_value :: :accept | :reject | :waive | :expired

  @type t :: %__MODULE__{
          decision_kind: pack_identifier(),
          description: String.t() | nil,
          trigger: decision_trigger(),
          required_evidence_kinds: [pack_identifier()],
          authorized_actors: [pack_identifier()],
          allowed_decisions: [decision_value()],
          required_within_hours: pos_integer() | nil
        }

  defstruct [
    :decision_kind,
    :description,
    :trigger,
    required_evidence_kinds: [],
    authorized_actors: [],
    allowed_decisions: [:accept, :reject],
    required_within_hours: nil
  ]
end

defmodule Mezzanine.Pack.EvidenceSpec do
  @moduledoc """
  Evidence definition for one collected proof artifact.
  """

  @type pack_identifier :: atom() | String.t()
  @type collection_strategy :: :automatic | :manual | :on_demand

  @type collection_trigger ::
          {:execution_completed, recipe_ref :: pack_identifier()}
          | {:decision_created, decision_kind :: pack_identifier()}
          | {:subject_entered_state, state :: pack_identifier()}

  @type t :: %__MODULE__{
          evidence_kind: pack_identifier(),
          description: String.t() | nil,
          collector_ref: pack_identifier(),
          collection_strategy: collection_strategy(),
          collected_on: collection_trigger(),
          schema: map() | nil
        }

  defstruct [
    :evidence_kind,
    :description,
    :collector_ref,
    :collected_on,
    collection_strategy: :automatic,
    schema: nil
  ]
end

defmodule Mezzanine.Pack.OperatorActionSpec do
  @moduledoc """
  Manual operator intervention supported by a pack.
  """

  @type pack_identifier :: atom() | String.t()

  @type effect ::
          {:advance_lifecycle, to_state :: pack_identifier()}
          | :block_subject
          | :unblock_subject
          | {:dispatch_effect, effect_kind :: pack_identifier()}
          | :cancel_active_execution
          | {:collect_evidence, evidence_kind :: pack_identifier()}

  @type t :: %__MODULE__{
          action_kind: pack_identifier(),
          description: String.t() | nil,
          applicable_states: [pack_identifier()],
          authorized_roles: [pack_identifier()],
          effect: effect()
        }

  defstruct [:action_kind, :description, :effect, applicable_states: [], authorized_roles: []]
end

defmodule Mezzanine.Pack.ProjectionSpec do
  @moduledoc """
  Named projection definition exposed to northbound query services.
  """

  @type pack_identifier :: atom() | String.t()
  @type filter_value :: atom() | [atom()] | String.t() | boolean()
  @type sort_dir :: :asc | :desc

  @type t :: %__MODULE__{
          name: pack_identifier(),
          description: String.t() | nil,
          subject_kinds: [pack_identifier()],
          default_filters: %{optional(pack_identifier()) => filter_value() | [filter_value()]},
          sort: [{pack_identifier(), sort_dir()}],
          included_fields: [pack_identifier()] | :all
        }

  defstruct [
    :name,
    description: nil,
    subject_kinds: [],
    default_filters: %{},
    sort: [{:inserted_at, :desc}],
    included_fields: :all
  ]
end
