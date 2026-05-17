defmodule Mezzanine.Pack.ValidationError do
  @moduledoc """
  Structured compiler diagnostic for pack validation.
  """

  @type severity :: :error | :warning

  @type t :: %__MODULE__{
          severity: severity(),
          path: [atom() | non_neg_integer()],
          message: String.t()
        }

  defstruct [:message, severity: :error, path: []]

  @spec error([atom() | non_neg_integer()], String.t()) :: t()
  def error(path, message), do: %__MODULE__{severity: :error, path: path, message: message}

  @spec warning([atom() | non_neg_integer()], String.t()) :: t()
  def warning(path, message), do: %__MODULE__{severity: :warning, path: path, message: message}
end

defmodule Mezzanine.Pack.SubjectContext do
  @moduledoc """
  Pure guard-evaluation context assembled ahead of lifecycle checks.
  """

  @type pack_identifier :: atom() | String.t()

  @type t :: %__MODULE__{
          payload: map(),
          evidence_summary: %{optional(pack_identifier()) => :collected | :pending | :failed},
          decisions: %{optional(pack_identifier()) => :accept | :reject | :waive | :expired}
        }

  defstruct payload: %{}, evidence_summary: %{}, decisions: %{}

  @spec from_snapshot(Mezzanine.Lifecycle.SubjectSnapshot.t()) :: t()
  def from_snapshot(snapshot) do
    %__MODULE__{
      payload: snapshot.payload,
      evidence_summary: snapshot.evidence_summary,
      decisions: snapshot.decisions
    }
  end
end

defmodule Mezzanine.Lifecycle.SubjectSnapshot do
  @moduledoc """
  Pure lifecycle input used before durable subject resources exist.
  """

  @type t :: %__MODULE__{
          subject_kind: String.t(),
          lifecycle_state: String.t(),
          payload: map(),
          evidence_summary: %{optional(String.t()) => :collected | :pending | :failed},
          decisions: %{optional(String.t()) => :accept | :reject | :waive | :expired}
        }

  defstruct [
    :subject_kind,
    :lifecycle_state,
    payload: %{},
    evidence_summary: %{},
    decisions: %{}
  ]

  @spec new(keyword() | map()) :: t()
  def new(attrs) when is_list(attrs), do: attrs |> Enum.into(%{}) |> new()

  def new(attrs) when is_map(attrs) do
    %__MODULE__{
      subject_kind: attrs |> fetch_required(:subject_kind) |> canonicalize_identifier!(),
      lifecycle_state: attrs |> fetch_required(:lifecycle_state) |> canonicalize_identifier!(),
      payload: Map.get(attrs, :payload, Map.get(attrs, "payload", %{})),
      evidence_summary:
        canonicalize_map_keys(
          Map.get(attrs, :evidence_summary, Map.get(attrs, "evidence_summary", %{}))
        ),
      decisions:
        canonicalize_map_keys(Map.get(attrs, :decisions, Map.get(attrs, "decisions", %{})))
    }
  end

  defp fetch_required(attrs, key) do
    Map.get(attrs, key) || Map.fetch!(attrs, Atom.to_string(key))
  end

  defp canonicalize_map_keys(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {canonicalize_identifier!(key), value} end)
  end

  defp canonicalize_identifier!(value) when is_atom(value), do: Atom.to_string(value)
  defp canonicalize_identifier!(value) when is_binary(value), do: value

  defp canonicalize_identifier!(value) do
    raise ArgumentError,
          "expected pack identifier to be an atom or string, got: #{inspect(value)}"
  end
end

defmodule Mezzanine.Pack.CompiledOperationRole do
  @moduledoc """
  Runtime operation role resolved from product role refs to binding and operation refs.
  """

  @type t :: %__MODULE__{
          role_ref: String.t(),
          binding_ref: String.t(),
          binding_kind: atom(),
          operation_role: String.t(),
          operation_ref: String.t(),
          operation_class: atom(),
          projection_order_key: pos_integer(),
          completion_policy: atom(),
          failure_policy: atom(),
          metadata: map()
        }

  @enforce_keys [
    :role_ref,
    :binding_ref,
    :binding_kind,
    :operation_role,
    :operation_ref,
    :operation_class,
    :projection_order_key
  ]
  defstruct @enforce_keys ++
              [completion_policy: :required, failure_policy: :fail_closed, metadata: %{}]
end

defmodule Mezzanine.Pack.CompiledOperationDependency do
  @moduledoc """
  Runtime operation dependency resolved to compiled product role refs.
  """

  @type t :: %__MODULE__{
          from_role: String.t(),
          to_role: String.t(),
          relation: atom(),
          completion_policy: atom(),
          failure_policy: atom(),
          review_policy_ref: String.t() | nil,
          confirmation_policy_ref: String.t() | nil,
          metadata: map()
        }

  @enforce_keys [:from_role, :to_role, :relation]
  defstruct @enforce_keys ++
              [
                completion_policy: :required,
                failure_policy: :fail_closed,
                review_policy_ref: nil,
                confirmation_policy_ref: nil,
                metadata: %{}
              ]
end

defmodule Mezzanine.Pack.CompiledOperationGraph do
  @moduledoc """
  Runtime operation graph resolved from pack workflow data.
  """

  alias Mezzanine.Pack.{CompiledOperationDependency, CompiledOperationRole}

  @type t :: %__MODULE__{
          graph_ref: String.t(),
          workflow_ref: String.t(),
          roles: [CompiledOperationRole.t()],
          roles_by_ref: %{String.t() => CompiledOperationRole.t()},
          dependencies: [CompiledOperationDependency.t()],
          joins: [map()],
          metadata: map()
        }

  @enforce_keys [:graph_ref, :workflow_ref, :roles, :dependencies]
  defstruct @enforce_keys ++ [roles_by_ref: %{}, joins: [], metadata: %{}]
end

defmodule Mezzanine.Pack.CompiledPack do
  @moduledoc """
  Normalized pack plus O(1) runtime lookup indices.
  """

  alias Mezzanine.Pack.{
    BindingSpec,
    CompiledOperationGraph,
    ContextSourceSpec,
    DecisionSpec,
    EvidenceBinding,
    EvidenceSpec,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    OperationGraph,
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
    ToolBinding,
    WorkflowSpec
  }

  @type trigger_key ::
          :auto
          | {:execution_requested, String.t()}
          | {:execution_completed, String.t()}
          | {:execution_failed, String.t()}
          | {:execution_failed, String.t(), atom()}
          | {:join_completed, String.t()}
          | {:decision_made, String.t(), atom()}
          | {:operator_action, String.t()}
          | {:subject_entered_state, String.t()}

  @type state_key :: {String.t(), String.t()}
  @type binding_kind :: BindingSpec.binding_kind()
  @type binding_record ::
          SourceBinding.t()
          | SourcePublicationBinding.t()
          | RuntimeBinding.t()
          | ToolBinding.t()
          | EvidenceBinding.t()
          | ResourceEffectBinding.t()

  @type t :: %__MODULE__{
          pack_slug: String.t(),
          version: String.t(),
          manifest: Manifest.t(),
          subject_kinds: %{String.t() => SubjectKindSpec.t()},
          source_kinds: %{String.t() => SourceKindSpec.t()},
          bindings_by_ref: %{String.t() => binding_record()},
          bindings_by_kind: %{binding_kind() => [binding_record()]},
          source_bindings_by_ref: %{String.t() => SourceBindingSpec.t()},
          source_publishers_by_ref: %{String.t() => SourcePublishSpec.t()},
          context_sources_by_ref: %{String.t() => ContextSourceSpec.t()},
          lifecycle_by_kind: %{String.t() => LifecycleSpec.t()},
          transitions_by_state: %{state_key() => %{trigger_key() => LifecycleSpec.transition()}},
          terminal_states_by_kind: %{String.t() => MapSet.t(String.t())},
          recipes_by_ref: %{String.t() => ExecutionRecipeSpec.t()},
          recipes_by_subject_kind: %{String.t() => [ExecutionRecipeSpec.t()]},
          operation_graphs_by_ref: %{String.t() => OperationGraph.t()},
          compiled_operation_graphs_by_ref: %{String.t() => CompiledOperationGraph.t()},
          workflows_by_ref: %{String.t() => WorkflowSpec.t()},
          decision_specs_by_kind: %{String.t() => DecisionSpec.t()},
          evidence_specs_by_kind: %{String.t() => EvidenceSpec.t()},
          operator_actions_by_kind: %{String.t() => OperatorActionSpec.t()},
          projections_by_name: %{String.t() => ProjectionSpec.t()},
          decision_triggers_by_event: %{trigger_key() => [DecisionSpec.t()]},
          evidence_triggers_by_event: %{trigger_key() => [EvidenceSpec.t()]}
        }

  defstruct [
    :pack_slug,
    :version,
    :manifest,
    subject_kinds: %{},
    source_kinds: %{},
    bindings_by_ref: %{},
    bindings_by_kind: %{},
    source_bindings_by_ref: %{},
    source_publishers_by_ref: %{},
    context_sources_by_ref: %{},
    lifecycle_by_kind: %{},
    transitions_by_state: %{},
    terminal_states_by_kind: %{},
    recipes_by_ref: %{},
    recipes_by_subject_kind: %{},
    operation_graphs_by_ref: %{},
    compiled_operation_graphs_by_ref: %{},
    workflows_by_ref: %{},
    decision_specs_by_kind: %{},
    evidence_specs_by_kind: %{},
    operator_actions_by_kind: %{},
    projections_by_name: %{},
    decision_triggers_by_event: %{},
    evidence_triggers_by_event: %{}
  ]

  @spec transitions_for(t(), atom() | String.t(), atom() | String.t()) ::
          %{trigger_key() => LifecycleSpec.transition()}
  def transitions_for(%__MODULE__{} = compiled, subject_kind, lifecycle_state) do
    Map.get(
      compiled.transitions_by_state,
      {canonicalize_identifier!(subject_kind), canonicalize_identifier!(lifecycle_state)},
      %{}
    )
  end

  @spec terminal_state?(t(), atom() | String.t(), atom() | String.t()) :: boolean()
  def terminal_state?(%__MODULE__{} = compiled, subject_kind, lifecycle_state) do
    subject_kind = canonicalize_identifier!(subject_kind)
    lifecycle_state = canonicalize_identifier!(lifecycle_state)

    compiled
    |> Map.get(:terminal_states_by_kind, %{})
    |> Map.get(subject_kind, MapSet.new())
    |> MapSet.member?(lifecycle_state)
  end

  defp canonicalize_identifier!(value) when is_atom(value), do: Atom.to_string(value)
  defp canonicalize_identifier!(value) when is_binary(value), do: value

  defp canonicalize_identifier!(value) do
    raise ArgumentError,
          "expected pack identifier to be an atom or string, got: #{inspect(value)}"
  end
end
