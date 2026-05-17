defmodule Mezzanine.Pack.Manifest do
  @moduledoc """
  Atomic pack definition returned from the `manifest/0` callback on `Mezzanine.Pack`.
  """

  alias Mezzanine.Pack.{
    BindingSpec,
    ContextSourceSpec,
    DecisionSpec,
    EvidenceBinding,
    EvidenceSpec,
    ExecutionRecipeSpec,
    LifecycleSpec,
    OperatorActionSpec,
    ProjectionSpec,
    ResourceEffectBinding,
    RuntimeBinding,
    SourceBinding,
    SourceBindingSpec,
    SourceKindSpec,
    SourcePublicationBinding,
    SourcePublishSpec,
    SubjectKindSpec,
    ToolBinding
  }

  @type pack_identifier :: atom() | String.t()
  @type migration_strategy :: :additive | :force
  @type profile_slots :: map()
  @type binding_record :: BindingSpec.binding_record()

  @type t :: %__MODULE__{
          pack_slug: pack_identifier(),
          version: String.t(),
          description: String.t() | nil,
          migration_strategy: migration_strategy(),
          max_supersession_depth: pos_integer(),
          profile_slots: profile_slots() | nil,
          subject_kind_specs: [SubjectKindSpec.t()],
          source_kind_specs: [SourceKindSpec.t()],
          binding_specs: [binding_record()],
          source_binding_specs: [SourceBindingSpec.t()],
          source_publish_specs: [SourcePublishSpec.t()],
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
    profile_slots: nil,
    subject_kind_specs: [],
    source_kind_specs: [],
    binding_specs: [],
    source_binding_specs: [],
    source_publish_specs: [],
    context_source_specs: [],
    lifecycle_specs: [],
    execution_recipe_specs: [],
    decision_specs: [],
    evidence_specs: [],
    operator_action_specs: [],
    projection_specs: []
  ]
end

defmodule Mezzanine.Pack.BindingSpec do
  @moduledoc """
  Generic binding discriminator for provider-neutral pack records.
  """

  alias Mezzanine.Pack.{
    EvidenceBinding,
    ResourceEffectBinding,
    RuntimeBinding,
    SourceBinding,
    SourcePublicationBinding,
    ToolBinding
  }

  @type binding_kind ::
          :source
          | :source_publication
          | :runtime
          | :runtime_tool
          | :evidence
          | :resource_effect

  @type binding_record ::
          SourceBinding.t()
          | SourcePublicationBinding.t()
          | RuntimeBinding.t()
          | ToolBinding.t()
          | EvidenceBinding.t()
          | ResourceEffectBinding.t()

  @spec kind(binding_record()) :: binding_kind()
  def kind(%{__struct__: SourceBinding}), do: :source
  def kind(%{__struct__: SourcePublicationBinding}), do: :source_publication
  def kind(%{__struct__: RuntimeBinding}), do: :runtime
  def kind(%{__struct__: ToolBinding}), do: :runtime_tool
  def kind(%{__struct__: EvidenceBinding}), do: :evidence
  def kind(%{__struct__: ResourceEffectBinding}), do: :resource_effect
end

defmodule Mezzanine.Pack.SourceBinding do
  @moduledoc """
  Generic source-reader binding declared by a product pack.
  """

  @type pack_identifier :: atom() | String.t()

  @type t :: %__MODULE__{
          binding_ref: pack_identifier(),
          source_kind: pack_identifier(),
          subject_kind: pack_identifier(),
          connector_ref: pack_identifier(),
          manifest_ref: pack_identifier(),
          operation_refs: %{required(pack_identifier()) => pack_identifier()},
          credential_binding_ref: pack_identifier(),
          adapter_ref: pack_identifier() | nil,
          connection_ref: pack_identifier() | nil,
          candidate_filter_ref: pack_identifier() | nil,
          cursor_policy_ref: pack_identifier() | nil,
          projection_profile_ref: pack_identifier() | nil,
          retry_policy_ref: pack_identifier() | nil,
          metadata: map()
        }

  @enforce_keys [
    :binding_ref,
    :source_kind,
    :subject_kind,
    :connector_ref,
    :manifest_ref,
    :operation_refs,
    :credential_binding_ref
  ]
  defstruct [
    :binding_ref,
    :source_kind,
    :subject_kind,
    :connector_ref,
    :manifest_ref,
    :operation_refs,
    :credential_binding_ref,
    :adapter_ref,
    :connection_ref,
    :candidate_filter_ref,
    :cursor_policy_ref,
    :projection_profile_ref,
    :retry_policy_ref,
    metadata: %{}
  ]
end

defmodule Mezzanine.Pack.SourcePublicationBinding do
  @moduledoc """
  Generic source-writer binding declared by a product pack.
  """

  @type pack_identifier :: atom() | String.t()
  @type idempotency_scope :: :subject | :execution | :source_event

  @type t :: %__MODULE__{
          binding_ref: pack_identifier(),
          source_binding_ref: pack_identifier(),
          connector_ref: pack_identifier(),
          manifest_ref: pack_identifier(),
          operation_refs: %{required(pack_identifier()) => pack_identifier()},
          credential_binding_ref: pack_identifier(),
          template_ref: pack_identifier(),
          idempotency_scope: idempotency_scope(),
          publication_profile_ref: pack_identifier() | nil,
          retry_policy_ref: pack_identifier() | nil,
          metadata: map()
        }

  @enforce_keys [
    :binding_ref,
    :source_binding_ref,
    :connector_ref,
    :manifest_ref,
    :operation_refs,
    :credential_binding_ref,
    :template_ref
  ]
  defstruct [
    :binding_ref,
    :source_binding_ref,
    :connector_ref,
    :manifest_ref,
    :operation_refs,
    :credential_binding_ref,
    :template_ref,
    :publication_profile_ref,
    :retry_policy_ref,
    idempotency_scope: :subject,
    metadata: %{}
  ]
end

defmodule Mezzanine.Pack.RuntimeBinding do
  @moduledoc """
  Generic lower-runtime binding declared by a product pack.
  """

  @type pack_identifier :: atom() | String.t()
  @type runtime_family :: :direct | :session | :workflow | :playbook | :scan | :inference

  @type t :: %__MODULE__{
          binding_ref: pack_identifier(),
          runtime_family: runtime_family(),
          connector_ref: pack_identifier(),
          manifest_ref: pack_identifier(),
          operation_refs: %{required(pack_identifier()) => pack_identifier()},
          credential_binding_ref: pack_identifier(),
          session_policy_ref: pack_identifier() | nil,
          tool_catalog_ref: pack_identifier() | nil,
          retry_policy_ref: pack_identifier() | nil,
          metadata: map()
        }

  @enforce_keys [
    :binding_ref,
    :runtime_family,
    :connector_ref,
    :manifest_ref,
    :operation_refs,
    :credential_binding_ref
  ]
  defstruct [
    :binding_ref,
    :runtime_family,
    :connector_ref,
    :manifest_ref,
    :operation_refs,
    :credential_binding_ref,
    :session_policy_ref,
    :tool_catalog_ref,
    :retry_policy_ref,
    metadata: %{}
  ]
end

defmodule Mezzanine.Pack.ToolBinding do
  @moduledoc """
  Generic runtime-tool binding declared by a product pack.
  """

  @type pack_identifier :: atom() | String.t()

  @type t :: %__MODULE__{
          binding_ref: pack_identifier(),
          runtime_binding_ref: pack_identifier(),
          connector_ref: pack_identifier(),
          manifest_ref: pack_identifier(),
          operation_refs: %{required(pack_identifier()) => pack_identifier()},
          authorization_class: pack_identifier(),
          credential_binding_ref: pack_identifier(),
          tool_schema_ref: pack_identifier() | nil,
          input_policy_ref: pack_identifier() | nil,
          retry_policy_ref: pack_identifier() | nil,
          metadata: map()
        }

  @enforce_keys [
    :binding_ref,
    :runtime_binding_ref,
    :connector_ref,
    :manifest_ref,
    :operation_refs,
    :authorization_class,
    :credential_binding_ref
  ]
  defstruct [
    :binding_ref,
    :runtime_binding_ref,
    :connector_ref,
    :manifest_ref,
    :operation_refs,
    :authorization_class,
    :credential_binding_ref,
    :tool_schema_ref,
    :input_policy_ref,
    :retry_policy_ref,
    metadata: %{}
  ]
end

defmodule Mezzanine.Pack.EvidenceBinding do
  @moduledoc """
  Generic evidence-collection binding declared by a product pack.
  """

  @type pack_identifier :: atom() | String.t()

  @type t :: %__MODULE__{
          binding_ref: pack_identifier(),
          evidence_kind: pack_identifier(),
          connector_ref: pack_identifier(),
          manifest_ref: pack_identifier(),
          operation_refs: %{required(pack_identifier()) => pack_identifier()},
          credential_binding_ref: pack_identifier(),
          collection_policy_ref: pack_identifier() | nil,
          retry_policy_ref: pack_identifier() | nil,
          metadata: map()
        }

  @enforce_keys [
    :binding_ref,
    :evidence_kind,
    :connector_ref,
    :manifest_ref,
    :operation_refs,
    :credential_binding_ref
  ]
  defstruct [
    :binding_ref,
    :evidence_kind,
    :connector_ref,
    :manifest_ref,
    :operation_refs,
    :credential_binding_ref,
    :collection_policy_ref,
    :retry_policy_ref,
    metadata: %{}
  ]
end

defmodule Mezzanine.Pack.ResourceEffectBinding do
  @moduledoc """
  Generic side-effect binding declared by a product pack.
  """

  @type pack_identifier :: atom() | String.t()

  @type t :: %__MODULE__{
          binding_ref: pack_identifier(),
          effect_kind: pack_identifier(),
          connector_ref: pack_identifier(),
          manifest_ref: pack_identifier(),
          operation_refs: %{required(pack_identifier()) => pack_identifier()},
          operation_group_ref: pack_identifier(),
          credential_binding_ref: pack_identifier(),
          confirmation_policy_ref: pack_identifier() | nil,
          retry_policy_ref: pack_identifier() | nil,
          metadata: map()
        }

  @enforce_keys [
    :binding_ref,
    :effect_kind,
    :connector_ref,
    :manifest_ref,
    :operation_refs,
    :operation_group_ref,
    :credential_binding_ref
  ]
  defstruct [
    :binding_ref,
    :effect_kind,
    :connector_ref,
    :manifest_ref,
    :operation_refs,
    :operation_group_ref,
    :credential_binding_ref,
    :confirmation_policy_ref,
    :retry_policy_ref,
    metadata: %{}
  ]
end

defmodule Mezzanine.Pack.SourceBindingSpec do
  @moduledoc """
  Installed source binding required by a pack.
  """

  @type pack_identifier :: atom() | String.t()

  @type t :: %__MODULE__{
          binding_ref: pack_identifier(),
          source_kind: pack_identifier(),
          subject_kind: pack_identifier(),
          provider: pack_identifier(),
          connection_ref: pack_identifier(),
          state_mapping: %{optional(pack_identifier()) => [String.t()]},
          candidate_filters: map(),
          cursor_policy: map(),
          source_write_policy: map()
        }

  defstruct [
    :binding_ref,
    :source_kind,
    :subject_kind,
    :provider,
    :connection_ref,
    state_mapping: %{},
    candidate_filters: %{},
    cursor_policy: %{},
    source_write_policy: %{}
  ]
end

defmodule Mezzanine.Pack.SourcePublishSpec do
  @moduledoc """
  Source-side publish rule emitted through lower connector effects.
  """

  @type pack_identifier :: atom() | String.t()
  @type operation ::
          :update_state | :create_comment | :update_comment | :add_label | :remove_label
  @type idempotency_scope :: :subject | :execution | :source_event

  @type trigger ::
          {:subject_entered_state, pack_identifier()}
          | {:execution_completed, pack_identifier()}
          | {:decision_made, pack_identifier(), atom()}
          | {:operator_action, pack_identifier()}

  @type t :: %__MODULE__{
          publish_ref: pack_identifier(),
          source_binding_ref: pack_identifier(),
          trigger: trigger(),
          operation: operation(),
          template_ref: pack_identifier() | nil,
          idempotency_scope: idempotency_scope()
        }

  defstruct [
    :publish_ref,
    :source_binding_ref,
    :trigger,
    :operation,
    :template_ref,
    idempotency_scope: :subject
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
          optional(:root_ref) => pack_identifier(),
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
          sandbox_policy_ref: pack_identifier() | nil,
          prompt_refs: [pack_identifier()],
          dynamic_tool_manifest: map(),
          hook_stages: [atom()],
          max_turns: pos_integer() | nil,
          stall_timeout_ms: pos_integer() | nil,
          dispatch_ref_requirements: map(),
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
    sandbox_policy_ref: nil,
    prompt_refs: [],
    dynamic_tool_manifest: %{},
    hook_stages: [],
    max_turns: nil,
    stall_timeout_ms: nil,
    dispatch_ref_requirements: %{
      authority_decision_ref: :required,
      connector_binding_ref: :required,
      credential_posture_ref: :credential_lease_or_no_credentials
    },
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

  @type decision_value :: :accept | :reject | :waive | :expired | :escalate

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
          | :pause_execution
          | :resume_execution
          | :retry_execution
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
