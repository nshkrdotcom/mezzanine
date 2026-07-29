defmodule Mezzanine.Runs.TurnCommand do
  @moduledoc "Canonical command for one durable follow-up agent turn."

  alias Mezzanine.Runs.ContractSupport, as: S

  @kinds [:user_input, :approval, :denial, :replan_hint, :rework_hint]
  @kind_lookup Map.new(@kinds, &{Atom.to_string(&1), &1})
  @forbidden_param_keys MapSet.new([
                          :api_key,
                          :body,
                          :credential,
                          :credentials,
                          :payload,
                          :provider_payload,
                          :raw_body,
                          :raw_payload,
                          :secret,
                          :token,
                          "api_key",
                          "body",
                          "credential",
                          "credentials",
                          "payload",
                          "provider_payload",
                          "raw_body",
                          "raw_payload",
                          "secret",
                          "token"
                        ])
  @fields [
    :command_ref,
    :idempotency_key,
    :request_hash,
    :tenant_ref,
    :actor_ref,
    :authority_ref,
    :run_ref,
    :turn_ref,
    :trace_ref,
    :correlation_ref,
    :kind,
    :payload_ref,
    :payload_digest,
    :cursor_ref,
    :pending_ref,
    :params
  ]
  @required @fields -- [:cursor_ref, :pending_ref]
  @enforce_keys @required
  defstruct @fields

  @type kind :: :user_input | :approval | :denial | :replan_hint | :rework_hint
  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, :invalid_turn_command}
  def new(%__MODULE__{} = command), do: validate(command)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = S.attrs(attrs)

    with :ok <- S.validate_fields(attrs, @fields, @required, :invalid_turn_command),
         {:ok, kind} <- normalize_kind(S.value(attrs, :kind)) do
      %__MODULE__{
        command_ref: S.value(attrs, :command_ref),
        idempotency_key: S.value(attrs, :idempotency_key),
        request_hash: S.value(attrs, :request_hash),
        tenant_ref: S.value(attrs, :tenant_ref),
        actor_ref: S.value(attrs, :actor_ref),
        authority_ref: S.value(attrs, :authority_ref),
        run_ref: S.value(attrs, :run_ref),
        turn_ref: S.value(attrs, :turn_ref),
        trace_ref: S.value(attrs, :trace_ref),
        correlation_ref: S.value(attrs, :correlation_ref),
        kind: kind,
        payload_ref: S.value(attrs, :payload_ref),
        payload_digest: S.value(attrs, :payload_digest),
        cursor_ref: S.value(attrs, :cursor_ref),
        pending_ref: S.value(attrs, :pending_ref),
        params: S.value(attrs, :params)
      }
      |> validate()
    end
  end

  def new(_attrs), do: {:error, :invalid_turn_command}

  @spec new!(map() | keyword() | t()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, command} -> command
      {:error, reason} -> raise ArgumentError, Atom.to_string(reason)
    end
  end

  def dump(%__MODULE__{} = command), do: S.dump(command)

  defp validate(%__MODULE__{} = command) do
    refs = [
      command.command_ref,
      command.tenant_ref,
      command.actor_ref,
      command.authority_ref,
      command.run_ref,
      command.turn_ref,
      command.trace_ref,
      command.correlation_ref,
      command.payload_ref
    ]

    optional_refs = [command.cursor_ref, command.pending_ref]

    if Enum.all?(refs, &S.safe_ref?/1) and
         Enum.all?(optional_refs, &(is_nil(&1) or S.safe_ref?(&1))) and
         S.present?(command.idempotency_key) and S.hash?(command.request_hash) and
         S.hash?(command.payload_digest) and command.kind in @kinds and
         safe_params?(command.params) do
      {:ok, command}
    else
      {:error, :invalid_turn_command}
    end
  end

  defp normalize_kind(kind) when is_atom(kind) do
    if kind in @kinds, do: {:ok, kind}, else: {:error, :invalid_turn_command}
  end

  defp normalize_kind(kind) when is_binary(kind) do
    case Map.fetch(@kind_lookup, kind) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, :invalid_turn_command}
    end
  end

  defp normalize_kind(_kind), do: {:error, :invalid_turn_command}

  defp safe_params?(params) when is_map(params) do
    Enum.all?(params, fn {key, value} ->
      not MapSet.member?(@forbidden_param_keys, key) and safe_param_value?(value)
    end)
  end

  defp safe_params?(_params), do: false

  defp safe_param_value?(value) when is_map(value), do: safe_params?(value)
  defp safe_param_value?(value) when is_list(value), do: Enum.all?(value, &safe_param_value?/1)

  defp safe_param_value?(value) when is_binary(value) or is_number(value) or is_boolean(value),
    do: true

  defp safe_param_value?(nil), do: true
  defp safe_param_value?(_value), do: false
end

defmodule Mezzanine.Runs.TurnAcceptance do
  @moduledoc "Durable acceptance and run-cursor identity for one follow-up turn."

  alias Mezzanine.Runs.{ContractSupport, EventCursor}

  @fields [
    :command_ref,
    :run_ref,
    :turn_ref,
    :event_ref,
    :signal_outbox_ref,
    :cursor,
    :run_revision,
    :state,
    :idempotent_replay?
  ]
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, :invalid_turn_acceptance}
  def new(%__MODULE__{} = acceptance), do: validate(acceptance)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = ContractSupport.attrs(attrs)

    with :ok <-
           ContractSupport.validate_fields(
             attrs,
             @fields,
             @fields,
             :invalid_turn_acceptance
           ),
         {:ok, cursor} <- EventCursor.new(ContractSupport.value(attrs, :cursor)) do
      %__MODULE__{
        command_ref: ContractSupport.value(attrs, :command_ref),
        run_ref: ContractSupport.value(attrs, :run_ref),
        turn_ref: ContractSupport.value(attrs, :turn_ref),
        event_ref: ContractSupport.value(attrs, :event_ref),
        signal_outbox_ref: ContractSupport.value(attrs, :signal_outbox_ref),
        cursor: cursor,
        run_revision: ContractSupport.value(attrs, :run_revision),
        state: normalize_state(ContractSupport.value(attrs, :state)),
        idempotent_replay?: ContractSupport.value(attrs, :idempotent_replay?)
      }
      |> validate()
    end
  end

  def new(_attrs), do: {:error, :invalid_turn_acceptance}

  @spec new!(map() | keyword() | t()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, acceptance} -> acceptance
      {:error, reason} -> raise ArgumentError, Atom.to_string(reason)
    end
  end

  def dump(%__MODULE__{} = acceptance), do: ContractSupport.dump(acceptance)

  defp validate(%__MODULE__{} = acceptance) do
    refs = [
      acceptance.command_ref,
      acceptance.run_ref,
      acceptance.turn_ref,
      acceptance.event_ref,
      acceptance.signal_outbox_ref
    ]

    if Enum.all?(refs, &ContractSupport.safe_ref?/1) and
         acceptance.cursor.run_ref == acceptance.run_ref and
         acceptance.cursor.last_event_ref == acceptance.event_ref and
         ContractSupport.positive_integer?(acceptance.run_revision) and
         acceptance.state == "accepted" and is_boolean(acceptance.idempotent_replay?) do
      {:ok, acceptance}
    else
      {:error, :invalid_turn_acceptance}
    end
  end

  defp normalize_state(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_state(value), do: value
end
