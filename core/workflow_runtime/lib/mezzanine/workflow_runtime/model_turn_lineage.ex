defmodule Mezzanine.WorkflowRuntime.ModelTurnStart do
  @moduledoc "Safe owner references required to begin one governed model turn."

  alias Mezzanine.Runs.ContractSupport, as: S

  @fields [
    :tenant_ref,
    :run_ref,
    :turn_ref,
    :context_artifact_ref,
    :context_digest,
    :prompt_artifact_ref,
    :decision_ref,
    :grant_ref,
    :provider_attempt_ref,
    :provider_family,
    :model_ref,
    :operation_ref
  ]
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, :invalid_model_turn_start}
  def new(%__MODULE__{} = start), do: validate(start)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = S.attrs(attrs)

    with :ok <- S.validate_fields(attrs, @fields, @fields, :invalid_model_turn_start) do
      %__MODULE__{
        tenant_ref: S.value(attrs, :tenant_ref),
        run_ref: S.value(attrs, :run_ref),
        turn_ref: S.value(attrs, :turn_ref),
        context_artifact_ref: S.value(attrs, :context_artifact_ref),
        context_digest: S.value(attrs, :context_digest),
        prompt_artifact_ref: S.value(attrs, :prompt_artifact_ref),
        decision_ref: S.value(attrs, :decision_ref),
        grant_ref: S.value(attrs, :grant_ref),
        provider_attempt_ref: S.value(attrs, :provider_attempt_ref),
        provider_family: normalize_string(S.value(attrs, :provider_family)),
        model_ref: S.value(attrs, :model_ref),
        operation_ref: S.value(attrs, :operation_ref)
      }
      |> validate()
    end
  end

  def new(_attrs), do: {:error, :invalid_model_turn_start}

  @spec new!(map() | keyword() | t()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, start} -> start
      {:error, reason} -> raise ArgumentError, Atom.to_string(reason)
    end
  end

  defp validate(%__MODULE__{} = start) do
    refs =
      Map.take(start, @fields -- [:context_digest, :provider_family])
      |> Map.values()

    if Enum.all?(refs, &S.safe_ref?/1) and S.hash?(start.context_digest) and
         S.present?(start.provider_family),
       do: {:ok, start},
       else: {:error, :invalid_model_turn_start}
  end

  defp normalize_string(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_string(value), do: value
end

defmodule Mezzanine.WorkflowRuntime.ProviderEvent do
  @moduledoc "Reference-only record of one real provider event boundary."

  alias Mezzanine.Runs.ContractSupport, as: S

  @streams ~w(assistant stdout stderr system control)
  @fields [
    :event_ref,
    :run_ref,
    :turn_ref,
    :provider_attempt_ref,
    :sequence,
    :event_type,
    :stream,
    :payload_ref,
    :payload_digest,
    :observed_at
  ]
  @enforce_keys @fields
  defstruct @fields ++ [:commit_state, :committed_at, :row_version]

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, :invalid_provider_event}
  def new(%__MODULE__{} = event), do: validate(event)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = S.attrs(attrs)

    with :ok <- S.validate_fields(attrs, @fields, @fields, :invalid_provider_event) do
      %__MODULE__{
        event_ref: S.value(attrs, :event_ref),
        run_ref: S.value(attrs, :run_ref),
        turn_ref: S.value(attrs, :turn_ref),
        provider_attempt_ref: S.value(attrs, :provider_attempt_ref),
        sequence: S.value(attrs, :sequence),
        event_type: normalize_string(S.value(attrs, :event_type)),
        stream: normalize_string(S.value(attrs, :stream)),
        payload_ref: S.value(attrs, :payload_ref),
        payload_digest: S.value(attrs, :payload_digest),
        observed_at: S.value(attrs, :observed_at),
        commit_state: "provisional",
        row_version: 1
      }
      |> validate()
    end
  end

  def new(_attrs), do: {:error, :invalid_provider_event}

  @spec new!(map() | keyword() | t()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, event} -> event
      {:error, reason} -> raise ArgumentError, Atom.to_string(reason)
    end
  end

  @spec from_store(map()) :: {:ok, t()} | {:error, :invalid_provider_event}
  def from_store(attrs) when is_map(attrs) do
    event = struct!(__MODULE__, attrs)
    validate(event)
  end

  defp validate(%__MODULE__{} = event) do
    refs = [
      event.event_ref,
      event.run_ref,
      event.turn_ref,
      event.provider_attempt_ref,
      event.payload_ref
    ]

    with true <- Enum.all?(refs, &S.safe_ref?/1),
         true <- S.positive_integer?(event.sequence),
         true <- S.present?(event.event_type),
         true <- event.stream in @streams,
         true <- S.hash?(event.payload_digest),
         true <- S.datetime?(event.observed_at),
         true <- event.commit_state in ~w(provisional committed),
         true <- is_nil(event.committed_at) or S.datetime?(event.committed_at),
         true <- S.positive_integer?(event.row_version) do
      {:ok, event}
    else
      _other -> {:error, :invalid_provider_event}
    end
  end

  defp normalize_string(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_string(value), do: value
end

defmodule Mezzanine.WorkflowRuntime.ModelTurnCompletion do
  @moduledoc "Terminal publication and continuation references for a successful model turn."

  alias Mezzanine.Runs.ContractSupport, as: S

  @fields [
    :turn_ref,
    :provider_attempt_ref,
    :reply_publication_ref,
    :reply_artifact_ref,
    :continuation_context_ref,
    :continuation_context_digest
  ]
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, :invalid_model_turn_completion}
  def new(%__MODULE__{} = completion), do: validate(completion)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = S.attrs(attrs)

    with :ok <-
           S.validate_fields(attrs, @fields, @fields, :invalid_model_turn_completion) do
      %__MODULE__{
        turn_ref: S.value(attrs, :turn_ref),
        provider_attempt_ref: S.value(attrs, :provider_attempt_ref),
        reply_publication_ref: S.value(attrs, :reply_publication_ref),
        reply_artifact_ref: S.value(attrs, :reply_artifact_ref),
        continuation_context_ref: S.value(attrs, :continuation_context_ref),
        continuation_context_digest: S.value(attrs, :continuation_context_digest)
      }
      |> validate()
    end
  end

  def new(_attrs), do: {:error, :invalid_model_turn_completion}

  @spec new!(map() | keyword() | t()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, completion} -> completion
      {:error, reason} -> raise ArgumentError, Atom.to_string(reason)
    end
  end

  defp validate(%__MODULE__{} = completion) do
    refs =
      completion
      |> Map.from_struct()
      |> Map.delete(:continuation_context_digest)
      |> Map.values()

    if Enum.all?(refs, &S.safe_ref?/1) and S.hash?(completion.continuation_context_digest),
      do: {:ok, completion},
      else: {:error, :invalid_model_turn_completion}
  end
end

defmodule Mezzanine.WorkflowRuntime.ModelTurnCursor do
  @moduledoc "Restart-safe cursor over committed provider event boundaries for one turn."

  alias Mezzanine.Runs.ContractSupport, as: S

  @enforce_keys [:turn_ref, :last_provider_event_ref, :sequence]
  defstruct [:turn_ref, :last_provider_event_ref, :sequence]

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, :invalid_model_turn_cursor}
  def new(%__MODULE__{} = cursor), do: validate(cursor)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = S.attrs(attrs)
    fields = [:turn_ref, :last_provider_event_ref, :sequence]

    with :ok <- S.validate_fields(attrs, fields, fields, :invalid_model_turn_cursor) do
      %__MODULE__{
        turn_ref: S.value(attrs, :turn_ref),
        last_provider_event_ref: S.value(attrs, :last_provider_event_ref),
        sequence: S.value(attrs, :sequence)
      }
      |> validate()
    end
  end

  def new(_attrs), do: {:error, :invalid_model_turn_cursor}

  @spec new!(map() | keyword() | t()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, cursor} -> cursor
      {:error, reason} -> raise ArgumentError, Atom.to_string(reason)
    end
  end

  defp validate(%__MODULE__{} = cursor) do
    if S.safe_ref?(cursor.turn_ref) and S.safe_ref?(cursor.last_provider_event_ref) and
         S.positive_integer?(cursor.sequence),
       do: {:ok, cursor},
       else: {:error, :invalid_model_turn_cursor}
  end
end
