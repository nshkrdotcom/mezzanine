defmodule Mezzanine.CoordinationEngine.VerifierPolicy.Decision do
  @moduledoc "Ref-only verifier termination decision."

  @enforce_keys [
    :verifier_policy_ref,
    :verifier_result_ref,
    :score_schema_ref,
    :termination_decision,
    :replay_ref,
    :trace_ref,
    :gepa_target_refs
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{}
end

defmodule Mezzanine.CoordinationEngine.VerifierPolicy do
  @moduledoc """
  Verifier and termination policy bound to model, operation, replay, and schema refs.
  """

  alias Mezzanine.CoordinationEngine.Validation
  alias Mezzanine.CoordinationEngine.VerifierPolicy.Decision

  @required_strings [
    :verifier_policy_ref,
    :verifier_prompt_ref,
    :verifier_model_profile_ref,
    :operation_policy_ref,
    :score_schema_ref,
    :termination_threshold_ref,
    :retry_policy_ref,
    :repair_policy_ref,
    :escalation_policy_ref,
    :replay_ref,
    :trace_ref
  ]

  @score_bands [:pass, :repair, :escalate, :terminate]

  @enforce_keys @required_strings ++ [:gepa_target_refs]
  defstruct @enforce_keys

  @type t :: %__MODULE__{}

  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    with :ok <- Validation.reject_raw(attrs),
         {:ok, strings} <- required_strings(attrs),
         {:ok, gepa_target_refs} <- Validation.require_string_list(attrs, :gepa_target_refs) do
      {:ok, struct!(__MODULE__, Map.put(strings, :gepa_target_refs, gepa_target_refs))}
    end
  end

  def new(_attrs), do: {:error, :invalid_verifier_policy}

  @spec evaluate(t(), map()) :: {:ok, Decision.t()} | {:error, term()}
  def evaluate(%__MODULE__{} = policy, attrs) when is_map(attrs) do
    with :ok <- Validation.reject_raw(attrs),
         {:ok, verifier_result_ref} <- Validation.require_binary(attrs, :verifier_result_ref),
         {:ok, score_schema_ref} <- Validation.require_binary(attrs, :score_schema_ref),
         :ok <- score_schema_matches(policy, score_schema_ref),
         {:ok, score_band} <- score_band(attrs),
         {:ok, replay_ref} <- Validation.require_binary(attrs, :replay_ref),
         {:ok, trace_ref} <- Validation.require_binary(attrs, :trace_ref) do
      {:ok,
       %Decision{
         verifier_policy_ref: policy.verifier_policy_ref,
         verifier_result_ref: verifier_result_ref,
         score_schema_ref: score_schema_ref,
         termination_decision: score_band,
         replay_ref: replay_ref,
         trace_ref: trace_ref,
         gepa_target_refs: policy.gepa_target_refs
       }}
    end
  end

  def evaluate(%__MODULE__{}, _attrs), do: {:error, :invalid_verifier_result}

  defp required_strings(attrs) do
    Enum.reduce_while(@required_strings, {:ok, %{}}, fn key, {:ok, acc} ->
      case Validation.require_binary(attrs, key) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, key, value)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp score_schema_matches(%__MODULE__{score_schema_ref: score_schema_ref}, score_schema_ref),
    do: :ok

  defp score_schema_matches(%__MODULE__{}, score_schema_ref),
    do: {:error, {:score_schema_mismatch, score_schema_ref}}

  defp score_band(attrs) do
    band = Validation.fetch(attrs, :score_band)

    if band in @score_bands do
      {:ok, band}
    else
      {:error, {:invalid_score_band, band}}
    end
  end
end
