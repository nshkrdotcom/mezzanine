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

defmodule Mezzanine.Pack.CompiledPack do
  @moduledoc """
  Normalized pack plus O(1) runtime lookup indices.
  """

  alias Mezzanine.Pack.{
    ContextSourceSpec,
    DecisionSpec,
    EvidenceSpec,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    OperatorActionSpec,
    ProjectionSpec,
    SourceKindSpec,
    SubjectKindSpec
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

  @type t :: %__MODULE__{
          pack_slug: String.t(),
          version: String.t(),
          manifest: Manifest.t(),
          subject_kinds: %{String.t() => SubjectKindSpec.t()},
          source_kinds: %{String.t() => SourceKindSpec.t()},
          context_sources_by_ref: %{String.t() => ContextSourceSpec.t()},
          lifecycle_by_kind: %{String.t() => LifecycleSpec.t()},
          transitions_by_state: %{state_key() => %{trigger_key() => LifecycleSpec.transition()}},
          terminal_states_by_kind: %{String.t() => MapSet.t(String.t())},
          recipes_by_ref: %{String.t() => ExecutionRecipeSpec.t()},
          recipes_by_subject_kind: %{String.t() => [ExecutionRecipeSpec.t()]},
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
    context_sources_by_ref: %{},
    lifecycle_by_kind: %{},
    transitions_by_state: %{},
    terminal_states_by_kind: %{},
    recipes_by_ref: %{},
    recipes_by_subject_kind: %{},
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
