defmodule Mezzanine.Runs.ContractSupport do
  @moduledoc false

  alias Mezzanine.AgentRuntime.Support

  def attrs(value) when is_list(value), do: Map.new(value)
  def attrs(%_{} = value), do: Map.from_struct(value)
  def attrs(value) when is_map(value), do: value
  def attrs(_value), do: %{}

  def value(attrs, key, default \\ nil),
    do: Map.get(attrs, key, Map.get(attrs, Atom.to_string(key), default))

  def validate_fields(attrs, allowed, required, error) do
    allowed_keys = MapSet.new(Enum.flat_map(allowed, &[&1, Atom.to_string(&1)]))

    with true <- Enum.all?(Map.keys(attrs), &MapSet.member?(allowed_keys, &1)),
         true <- Enum.all?(required, &(attrs |> value(&1) |> required_value?())),
         :ok <- Support.reject_unsafe(attrs, error) do
      :ok
    else
      _other -> {:error, error}
    end
  end

  def safe_ref?(value), do: Support.safe_ref?(value)
  def present?(value), do: Support.present_string?(value)
  def required_value?(value) when is_binary(value), do: present?(value)
  def required_value?(value), do: not is_nil(value)
  def positive_integer?(value), do: is_integer(value) and value > 0
  def non_negative_integer?(value), do: is_integer(value) and value >= 0

  def hash?("sha256:" <> hex),
    do: byte_size(hex) == 64 and String.match?(hex, ~r/\A[0-9a-f]{64}\z/)

  def hash?(_value), do: false
  def datetime?(%DateTime{}), do: true
  def datetime?(_value), do: false

  def dump(%_{} = value) do
    value
    |> Map.from_struct()
    |> Map.reject(fn {_key, nested} -> is_nil(nested) end)
    |> Support.dump_value()
  end
end

defmodule Mezzanine.Runs.FirstTurn do
  @moduledoc "Initial durable turn accepted with a run command."

  alias Mezzanine.Runs.ContractSupport, as: S

  @fields [
    :turn_ref,
    :subject_ref,
    :input_artifact_ref,
    :payload_digest,
    :idempotency_key,
    :sequence,
    :row_version
  ]
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(%__MODULE__{} = turn), do: validate(turn)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = S.attrs(attrs)

    with :ok <- S.validate_fields(attrs, @fields, @fields, :invalid_first_turn) do
      attrs
      |> Map.new(fn {key, value} -> {normalize_key(key), value} end)
      |> then(&struct!(__MODULE__, &1))
      |> validate()
    end
  end

  def new(_attrs), do: {:error, :invalid_first_turn}
  def new!(attrs), do: bang(new(attrs), :invalid_first_turn)
  def dump(%__MODULE__{} = turn), do: S.dump(turn)

  defp validate(%__MODULE__{} = turn) do
    refs = [turn.turn_ref, turn.subject_ref, turn.input_artifact_ref]

    if Enum.all?(refs, &S.safe_ref?/1) and S.present?(turn.idempotency_key) and
         S.hash?(turn.payload_digest) and turn.sequence == 1 and turn.row_version == 1 do
      {:ok, turn}
    else
      {:error, :invalid_first_turn}
    end
  end

  defp normalize_key(key) when is_binary(key), do: String.to_existing_atom(key)
  defp normalize_key(key), do: key
  defp bang({:ok, value}, _error), do: value
  defp bang({:error, error}, _default), do: raise(ArgumentError, Atom.to_string(error))
end

defmodule Mezzanine.Runs.AcceptCommand do
  @moduledoc "Canonical Mezzanine run and first-turn acceptance command."

  alias Mezzanine.Runs.{ContractSupport, FirstTurn}

  @fields [
    :command_ref,
    :idempotency_key,
    :request_hash,
    :tenant_ref,
    :installation_ref,
    :actor_ref,
    :program_id,
    :work_class_id,
    :subject_ref,
    :run_ref,
    :trace_ref,
    :correlation_ref,
    :authority_context_ref,
    :runtime_profile_ref,
    :tool_catalog_ref,
    :budget_ref,
    :deadline_at,
    :expected_revision,
    :first_turn
  ]
  @required @fields -- [:deadline_at]
  @enforce_keys @required
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(%__MODULE__{} = command), do: validate(command)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = ContractSupport.attrs(attrs)

    with :ok <-
           ContractSupport.validate_fields(attrs, @fields, @required, :invalid_accept_command),
         {:ok, first_turn} <- FirstTurn.new(ContractSupport.value(attrs, :first_turn)) do
      command = %__MODULE__{
        command_ref: ContractSupport.value(attrs, :command_ref),
        idempotency_key: ContractSupport.value(attrs, :idempotency_key),
        request_hash: ContractSupport.value(attrs, :request_hash),
        tenant_ref: ContractSupport.value(attrs, :tenant_ref),
        installation_ref: ContractSupport.value(attrs, :installation_ref),
        actor_ref: ContractSupport.value(attrs, :actor_ref),
        program_id: ContractSupport.value(attrs, :program_id),
        work_class_id: ContractSupport.value(attrs, :work_class_id),
        subject_ref: ContractSupport.value(attrs, :subject_ref),
        run_ref: ContractSupport.value(attrs, :run_ref),
        trace_ref: ContractSupport.value(attrs, :trace_ref),
        correlation_ref: ContractSupport.value(attrs, :correlation_ref),
        authority_context_ref: ContractSupport.value(attrs, :authority_context_ref),
        runtime_profile_ref: ContractSupport.value(attrs, :runtime_profile_ref),
        tool_catalog_ref: ContractSupport.value(attrs, :tool_catalog_ref),
        budget_ref: ContractSupport.value(attrs, :budget_ref),
        deadline_at: ContractSupport.value(attrs, :deadline_at),
        expected_revision: ContractSupport.value(attrs, :expected_revision),
        first_turn: first_turn
      }

      validate(command)
    end
  end

  def new(_attrs), do: {:error, :invalid_accept_command}
  def new!(attrs), do: bang(new(attrs))
  def dump(%__MODULE__{} = command), do: ContractSupport.dump(command)

  defp validate(%__MODULE__{} = command) do
    refs = [
      command.command_ref,
      command.tenant_ref,
      command.installation_ref,
      command.actor_ref,
      command.program_id,
      command.work_class_id,
      command.subject_ref,
      command.run_ref,
      command.trace_ref,
      command.correlation_ref,
      command.authority_context_ref,
      command.runtime_profile_ref,
      command.tool_catalog_ref,
      command.budget_ref
    ]

    with true <- Enum.all?(refs, &ContractSupport.safe_ref?/1),
         true <- ContractSupport.present?(command.idempotency_key),
         true <- ContractSupport.hash?(command.request_hash),
         true <- command.expected_revision == 0,
         true <- is_nil(command.deadline_at) or ContractSupport.datetime?(command.deadline_at),
         {:ok, _turn} <- FirstTurn.new(command.first_turn) do
      {:ok, command}
    else
      _other -> {:error, :invalid_accept_command}
    end
  end

  defp bang({:ok, value}), do: value
  defp bang({:error, error}), do: raise(ArgumentError, Atom.to_string(error))
end

defmodule Mezzanine.Runs.Event do
  @moduledoc "Append-only ordered business event for the canonical run aggregate."

  alias Mezzanine.Runs.ContractSupport, as: S

  @event_types ~w(run_accepted turn_accepted workflow_start_requested workflow_started)
  @fields [
    :event_ref,
    :run_ref,
    :tenant_ref,
    :event_type,
    :event_version,
    :sequence,
    :command_ref,
    :causation_ref,
    :correlation_ref,
    :payload_ref,
    :payload_digest,
    :recorded_at,
    :row_version
  ]
  @required @fields -- [:causation_ref]
  @enforce_keys @required
  defstruct @fields

  @type t :: %__MODULE__{}

  def event_types, do: @event_types

  def new(%__MODULE__{} = event), do: validate(event)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = S.attrs(attrs)

    with :ok <- S.validate_fields(attrs, @fields, @required, :invalid_run_event) do
      event = %__MODULE__{
        event_ref: S.value(attrs, :event_ref),
        run_ref: S.value(attrs, :run_ref),
        tenant_ref: S.value(attrs, :tenant_ref),
        event_type: normalize_string(S.value(attrs, :event_type)),
        event_version: S.value(attrs, :event_version),
        sequence: S.value(attrs, :sequence),
        command_ref: S.value(attrs, :command_ref),
        causation_ref: S.value(attrs, :causation_ref),
        correlation_ref: S.value(attrs, :correlation_ref),
        payload_ref: S.value(attrs, :payload_ref),
        payload_digest: S.value(attrs, :payload_digest),
        recorded_at: S.value(attrs, :recorded_at),
        row_version: S.value(attrs, :row_version)
      }

      validate(event)
    end
  end

  def new(_attrs), do: {:error, :invalid_run_event}
  def new!(attrs), do: bang(new(attrs))
  def dump(%__MODULE__{} = event), do: S.dump(event)

  defp validate(%__MODULE__{} = event) do
    refs = [
      event.event_ref,
      event.run_ref,
      event.tenant_ref,
      event.command_ref,
      event.correlation_ref,
      event.payload_ref
    ]

    optional_ref? = is_nil(event.causation_ref) or S.safe_ref?(event.causation_ref)

    if Enum.all?(refs, &S.safe_ref?/1) and optional_ref? and event.event_type in @event_types and
         event.event_version == 1 and S.positive_integer?(event.sequence) and
         S.hash?(event.payload_digest) and S.datetime?(event.recorded_at) and
         S.positive_integer?(event.row_version) do
      {:ok, event}
    else
      {:error, :invalid_run_event}
    end
  end

  defp normalize_string(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_string(value), do: value
  defp bang({:ok, value}), do: value
  defp bang({:error, error}), do: raise(ArgumentError, Atom.to_string(error))
end

defmodule Mezzanine.Runs.EventCursor do
  @moduledoc "Durable reconnect cursor for one canonical run event stream."

  alias Mezzanine.Runs.ContractSupport, as: S

  @enforce_keys [:run_ref, :last_event_ref, :sequence]
  defstruct [:run_ref, :last_event_ref, :sequence]

  @type t :: %__MODULE__{}

  def new(%__MODULE__{} = cursor), do: validate(cursor)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = S.attrs(attrs)
    fields = [:run_ref, :last_event_ref, :sequence]

    with :ok <- S.validate_fields(attrs, fields, fields, :invalid_event_cursor) do
      %__MODULE__{
        run_ref: S.value(attrs, :run_ref),
        last_event_ref: S.value(attrs, :last_event_ref),
        sequence: S.value(attrs, :sequence)
      }
      |> validate()
    end
  end

  def new(_attrs), do: {:error, :invalid_event_cursor}
  def new!(attrs), do: bang(new(attrs))
  def dump(%__MODULE__{} = cursor), do: S.dump(cursor)

  def advance(%__MODULE__{} = cursor, %Mezzanine.Runs.Event{} = event) do
    cond do
      event.run_ref != cursor.run_ref ->
        {:error, :cursor_run_mismatch}

      event.sequence != cursor.sequence + 1 ->
        {:error, :non_contiguous_event}

      true ->
        new(run_ref: cursor.run_ref, last_event_ref: event.event_ref, sequence: event.sequence)
    end
  end

  defp validate(%__MODULE__{} = cursor) do
    if S.safe_ref?(cursor.run_ref) and S.safe_ref?(cursor.last_event_ref) and
         S.positive_integer?(cursor.sequence),
       do: {:ok, cursor},
       else: {:error, :invalid_event_cursor}
  end

  defp bang({:ok, value}), do: value
  defp bang({:error, error}), do: raise(ArgumentError, Atom.to_string(error))
end

defmodule Mezzanine.Runs.WorkflowHandoff do
  @moduledoc "Durable outbox-to-Temporal workflow start handoff."

  alias Mezzanine.Runs.ContractSupport, as: S

  @states ~w(pending dispatched acknowledged ambiguous failed)
  @transitions %{
    "pending" => ~w(dispatched failed),
    "dispatched" => ~w(acknowledged ambiguous),
    "ambiguous" => ~w(acknowledged failed),
    "acknowledged" => [],
    "failed" => []
  }
  @fields [
    :outbox_ref,
    :event_ref,
    :run_ref,
    :workflow_ref,
    :workflow_type,
    :temporal_namespace,
    :task_queue,
    :idempotency_key,
    :state,
    :attempt,
    :last_error_ref
  ]
  @required @fields -- [:last_error_ref]
  @enforce_keys @required
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(%__MODULE__{} = handoff), do: validate(handoff)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = S.attrs(attrs)

    with :ok <- S.validate_fields(attrs, @fields, @required, :invalid_workflow_handoff) do
      %__MODULE__{
        outbox_ref: S.value(attrs, :outbox_ref),
        event_ref: S.value(attrs, :event_ref),
        run_ref: S.value(attrs, :run_ref),
        workflow_ref: S.value(attrs, :workflow_ref),
        workflow_type: S.value(attrs, :workflow_type),
        temporal_namespace: S.value(attrs, :temporal_namespace),
        task_queue: S.value(attrs, :task_queue),
        idempotency_key: S.value(attrs, :idempotency_key),
        state: normalize_string(S.value(attrs, :state)),
        attempt: S.value(attrs, :attempt),
        last_error_ref: S.value(attrs, :last_error_ref)
      }
      |> validate()
    end
  end

  def new(_attrs), do: {:error, :invalid_workflow_handoff}
  def new!(attrs), do: bang(new(attrs))
  def dump(%__MODULE__{} = handoff), do: S.dump(handoff)

  def transition(%__MODULE__{} = handoff, next_state, error_ref \\ nil) do
    next_state = normalize_string(next_state)

    if next_state in Map.fetch!(@transitions, handoff.state) do
      new(%{
        handoff
        | state: next_state,
          attempt: handoff.attempt + if(next_state == "dispatched", do: 1, else: 0),
          last_error_ref: error_ref
      })
    else
      {:error, :invalid_workflow_handoff_transition}
    end
  end

  defp validate(%__MODULE__{} = handoff) do
    refs = [handoff.outbox_ref, handoff.event_ref, handoff.run_ref, handoff.workflow_ref]
    optional_ref? = is_nil(handoff.last_error_ref) or S.safe_ref?(handoff.last_error_ref)

    if Enum.all?(refs, &S.safe_ref?/1) and S.present?(handoff.workflow_type) and
         S.present?(handoff.temporal_namespace) and S.present?(handoff.task_queue) and
         S.present?(handoff.idempotency_key) and handoff.state in @states and
         S.non_negative_integer?(handoff.attempt) and optional_ref? do
      {:ok, handoff}
    else
      {:error, :invalid_workflow_handoff}
    end
  end

  defp normalize_string(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_string(value), do: value
  defp bang({:ok, value}), do: value
  defp bang({:error, error}), do: raise(ArgumentError, Atom.to_string(error))
end

defmodule Mezzanine.Runs.Acceptance do
  @moduledoc "Result of the canonical run transaction, including durable cursor and handoff refs."

  alias Mezzanine.Runs.{ContractSupport, EventCursor}

  @states ~w(accepted ambiguous)
  @fields [
    :command_ref,
    :run_ref,
    :turn_ref,
    :event_ref,
    :workflow_outbox_ref,
    :cursor,
    :run_revision,
    :state
  ]
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(%__MODULE__{} = acceptance), do: validate(acceptance)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = ContractSupport.attrs(attrs)

    with :ok <-
           ContractSupport.validate_fields(attrs, @fields, @fields, :invalid_run_acceptance),
         {:ok, cursor} <- EventCursor.new(ContractSupport.value(attrs, :cursor)) do
      %__MODULE__{
        command_ref: ContractSupport.value(attrs, :command_ref),
        run_ref: ContractSupport.value(attrs, :run_ref),
        turn_ref: ContractSupport.value(attrs, :turn_ref),
        event_ref: ContractSupport.value(attrs, :event_ref),
        workflow_outbox_ref: ContractSupport.value(attrs, :workflow_outbox_ref),
        cursor: cursor,
        run_revision: ContractSupport.value(attrs, :run_revision),
        state: normalize_string(ContractSupport.value(attrs, :state))
      }
      |> validate()
    end
  end

  def new(_attrs), do: {:error, :invalid_run_acceptance}
  def new!(attrs), do: bang(new(attrs))
  def dump(%__MODULE__{} = acceptance), do: ContractSupport.dump(acceptance)

  defp validate(%__MODULE__{} = acceptance) do
    refs = [
      acceptance.command_ref,
      acceptance.run_ref,
      acceptance.turn_ref,
      acceptance.event_ref,
      acceptance.workflow_outbox_ref
    ]

    if Enum.all?(refs, &ContractSupport.safe_ref?/1) and
         acceptance.cursor.run_ref == acceptance.run_ref and
         acceptance.cursor.last_event_ref == acceptance.event_ref and
         ContractSupport.positive_integer?(acceptance.run_revision) and
         acceptance.state in @states do
      {:ok, acceptance}
    else
      {:error, :invalid_run_acceptance}
    end
  end

  defp normalize_string(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_string(value), do: value
  defp bang({:ok, value}), do: value
  defp bang({:error, error}), do: raise(ArgumentError, Atom.to_string(error))
end
