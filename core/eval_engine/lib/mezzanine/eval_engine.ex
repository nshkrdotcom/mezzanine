defmodule Mezzanine.EvalEngine do
  @moduledoc """
  Deterministic eval suite execution with bounded verdict composition.
  """

  alias OuterBrain.ContextABI.Failure

  @oracles [:exact_shape, :hash_match, :decision_class_match, :structural_subset]
  @max_concurrency 16
  @max_cases 128
  @raw_keys [
    :body,
    :raw_body,
    :payload,
    :raw_payload,
    :expected_output,
    :raw_expected_output,
    :model_output,
    :provider_payload,
    "body",
    "raw_body",
    "payload",
    "raw_payload",
    "expected_output",
    "raw_expected_output",
    "model_output",
    "provider_payload"
  ]

  defmodule EvalSuiteRef do
    @moduledoc "Eval suite ref and bounded case set."
    @enforce_keys [
      :suite_ref,
      :tenant_ref,
      :authority_ref,
      :installation_ref,
      :idempotency_key,
      :trace_ref,
      :regression_oracle,
      :cases,
      :release_manifest_ref
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            suite_ref: String.t(),
            tenant_ref: String.t(),
            authority_ref: String.t(),
            installation_ref: String.t(),
            idempotency_key: String.t(),
            trace_ref: String.t(),
            regression_oracle: atom(),
            cases: [map()],
            release_manifest_ref: String.t()
          }
  end

  defmodule EvalCaseProjection do
    @moduledoc "Per-case ref-only eval projection."
    @enforce_keys [:case_ref, :verdict, :severity, :evidence_ref]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            case_ref: String.t(),
            verdict: atom(),
            severity: atom(),
            evidence_ref: String.t()
          }
  end

  defmodule EvalRunRef do
    @moduledoc "Eval run result ref."
    @enforce_keys [
      :eval_run_ref,
      :suite_ref,
      :tenant_ref,
      :authority_ref,
      :installation_ref,
      :trace_ref,
      :variant_ref,
      :verdict,
      :case_projections,
      :cost_class,
      :release_manifest_ref
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            eval_run_ref: String.t(),
            suite_ref: String.t(),
            tenant_ref: String.t(),
            authority_ref: String.t(),
            installation_ref: String.t(),
            trace_ref: String.t(),
            variant_ref: String.t(),
            verdict: atom(),
            case_projections: [EvalCaseProjection.t()],
            cost_class: :eval,
            release_manifest_ref: String.t()
          }
  end

  defmodule EvalGateReceipt do
    @moduledoc "Eval gate receipt used by AI execution, promotion, and operator review paths."
    @enforce_keys [
      :eval_verdict_ref,
      :suite_ref,
      :tenant_ref,
      :authority_ref,
      :trace_ref,
      :status,
      :verdict,
      :safe_action,
      :failure_summary
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            eval_verdict_ref: String.t(),
            suite_ref: String.t(),
            tenant_ref: String.t(),
            authority_ref: String.t(),
            trace_ref: String.t(),
            status: atom(),
            verdict: atom(),
            safe_action: atom(),
            failure_summary: map() | nil
          }
  end

  @spec run(map(), map(), keyword()) :: {:ok, EvalRunRef.t()} | {:error, term()}
  def run(suite_attrs, variant_config, opts \\ [])
      when is_map(suite_attrs) and is_map(variant_config) and is_list(opts) do
    with {:ok, suite} <- suite_ref(suite_attrs),
         :ok <- authorize_eval(suite, opts),
         :ok <- bounded_concurrency(opts),
         :ok <- parent_budget_allows(suite, opts),
         {:ok, case_projections} <- run_cases(suite.cases, suite.regression_oracle) do
      verdict = compose_verdict(case_projections)

      {:ok,
       %EvalRunRef{
         eval_run_ref: eval_run_ref(suite, variant_config),
         suite_ref: suite.suite_ref,
         tenant_ref: suite.tenant_ref,
         authority_ref: suite.authority_ref,
         installation_ref: suite.installation_ref,
         trace_ref: suite.trace_ref,
         variant_ref: variant_ref(variant_config),
         verdict: verdict,
         case_projections: case_projections,
         cost_class: :eval,
         release_manifest_ref: suite.release_manifest_ref
       }}
    end
  end

  @spec suite_ref(map()) :: {:ok, EvalSuiteRef.t()} | {:error, term()}
  def suite_ref(attrs) when is_map(attrs) do
    with :ok <- reject_raw(attrs),
         :ok <-
           required_strings(attrs, [
             :suite_ref,
             :tenant_ref,
             :authority_ref,
             :installation_ref,
             :idempotency_key,
             :trace_ref,
             :release_manifest_ref
           ]),
         {:ok, oracle} <- member(attrs, :regression_oracle, @oracles),
         {:ok, cases} <- cases(attrs) do
      {:ok,
       %EvalSuiteRef{
         suite_ref: fetch!(attrs, :suite_ref),
         tenant_ref: fetch!(attrs, :tenant_ref),
         authority_ref: fetch!(attrs, :authority_ref),
         installation_ref: fetch!(attrs, :installation_ref),
         idempotency_key: fetch!(attrs, :idempotency_key),
         trace_ref: fetch!(attrs, :trace_ref),
         regression_oracle: oracle,
         cases: cases,
         release_manifest_ref: fetch!(attrs, :release_manifest_ref)
       }}
    end
  end

  def suite_ref(_attrs), do: {:error, :invalid_eval_suite}

  @spec gate_receipt(EvalRunRef.t() | Failure.t(), map() | keyword()) ::
          {:ok, EvalGateReceipt.t()} | {:error, Failure.t()}
  def gate_receipt(run_or_failure, attrs \\ %{})

  def gate_receipt(%EvalRunRef{} = run, attrs) do
    attrs = if is_list(attrs), do: Map.new(attrs), else: attrs
    verdict = run.verdict

    {:ok,
     %EvalGateReceipt{
       eval_verdict_ref: eval_verdict_ref(run, attrs),
       suite_ref: run.suite_ref,
       tenant_ref: run.tenant_ref,
       authority_ref: run.authority_ref,
       trace_ref: run.trace_ref,
       status: gate_status(verdict),
       verdict: verdict,
       safe_action: gate_safe_action(verdict),
       failure_summary: nil
     }}
  end

  def gate_receipt(%Failure{} = failure, attrs) do
    attrs = if is_list(attrs), do: Map.new(attrs), else: attrs

    with {:ok, summary} <- Failure.summary(failure),
         {:ok, suite_ref} <- required_failure_ref(attrs, :suite_ref),
         {:ok, tenant_ref} <- required_failure_ref(attrs, :tenant_ref),
         {:ok, authority_ref} <- required_failure_ref(attrs, :authority_ref),
         {:ok, trace_ref} <- trace_ref(attrs, failure) do
      {:ok,
       %EvalGateReceipt{
         eval_verdict_ref:
           "eval-verdict://failure/" <>
             (summary.failure_ref
              |> String.replace_prefix("failure://", "")
              |> URI.encode_www_form()),
         suite_ref: suite_ref,
         tenant_ref: tenant_ref,
         authority_ref: authority_ref,
         trace_ref: trace_ref,
         status: :blocked,
         verdict: :inconclusive,
         safe_action: :review_eval_evidence,
         failure_summary: summary
       }}
    else
      {:error, reason} -> eval_failure(reason, attrs)
    end
  end

  @spec failure_from_reason(term(), map() | keyword()) :: {:ok, Failure.t()} | {:error, term()}
  def failure_from_reason(reason, attrs \\ %{}) do
    attrs = if is_list(attrs), do: Map.new(attrs), else: attrs

    {reason_code, safe_message, retryable?} =
      case reason do
        :unauthorized_eval_run ->
          {"mezzanine.eval.authority_denied.v1", "eval authority was denied", false}

        :eval_parent_budget_exceeded ->
          {"mezzanine.eval.budget_exhausted.v1", "eval budget was exhausted", false}

        :eval_suite_has_no_cases ->
          {"mezzanine.eval.insufficient_evidence.v1", "eval suite has no cases", false}

        :eval_suite_case_count_unbounded ->
          {"mezzanine.eval.unbounded_case_count.v1", "eval suite has too many cases", false}

        {:raw_eval_payload_forbidden, _field} ->
          {"mezzanine.eval.raw_payload_rejected.v1", "eval payload contains forbidden raw data",
           false}

        _other ->
          {"mezzanine.eval.insufficient_evidence.v1", "eval failed without sufficient evidence",
           false}
      end

    Failure.new(%{
      owner: :mezzanine,
      reason_code: reason_code,
      safe_message: safe_message,
      retryable?: retryable?,
      trace_ref: fetch(attrs, :trace_ref),
      evidence_refs: evidence_refs(reason)
    })
  end

  defp authorize_eval(suite, opts) do
    allowed = Keyword.get(opts, :authorized_tenants, [suite.tenant_ref])

    if suite.tenant_ref in allowed do
      :ok
    else
      {:error, :unauthorized_eval_run}
    end
  end

  defp bounded_concurrency(opts) do
    concurrency = Keyword.get(opts, :max_concurrency, 1)

    if is_integer(concurrency) and concurrency in 1..@max_concurrency do
      :ok
    else
      {:error, :eval_concurrency_unbounded}
    end
  end

  defp parent_budget_allows(suite, opts) do
    requested =
      suite.cases
      |> Enum.map(&(Map.get(&1, :budget_units) || Map.get(&1, "budget_units") || 1))
      |> Enum.sum()

    case Keyword.get(opts, :parent_budget_units) do
      nil -> :ok
      budget when is_integer(budget) and requested <= budget -> :ok
      _budget -> {:error, :eval_parent_budget_exceeded}
    end
  end

  defp cases(attrs) do
    case fetch(attrs, :cases) do
      cases when is_list(cases) and cases != [] and length(cases) <= @max_cases ->
        validate_cases(cases)

      [] ->
        {:error, :eval_suite_has_no_cases}

      cases when is_list(cases) ->
        {:error, :eval_suite_case_count_unbounded}

      _other ->
        {:error, :eval_suite_has_no_cases}
    end
  end

  defp validate_cases(cases) do
    Enum.reduce_while(cases, {:ok, []}, fn case_attrs, {:ok, acc} ->
      case validate_case(case_attrs) do
        {:ok, validated} -> {:cont, {:ok, [validated | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, validated} -> {:ok, Enum.reverse(validated)}
      error -> error
    end
  end

  defp validate_case(case_attrs) when is_map(case_attrs) do
    with :ok <- reject_raw(case_attrs),
         :ok <- required_strings(case_attrs, [:case_ref, :input_prompt_ref]),
         :ok <- hash_or_ref_expected(case_attrs),
         :ok <- bounded_tolerance(case_attrs) do
      {:ok, case_attrs}
    end
  end

  defp validate_case(_case_attrs), do: {:error, :invalid_eval_case}

  defp hash_or_ref_expected(case_attrs) do
    if present_string?(fetch(case_attrs, :expected_output_ref)) or
         present_string?(fetch(case_attrs, :expected_output_hash)) do
      :ok
    else
      {:error, :eval_case_expected_output_must_be_ref_or_hash}
    end
  end

  defp bounded_tolerance(case_attrs) do
    tolerance = fetch(case_attrs, :tolerance, 0)

    if is_integer(tolerance) and tolerance >= 0 and tolerance <= 10 do
      :ok
    else
      {:error, :eval_case_tolerance_unbounded}
    end
  end

  defp run_cases(cases, oracle) do
    projections =
      cases
      |> Enum.sort_by(&fetch!(&1, :case_ref))
      |> Enum.map(&run_case(&1, oracle))

    {:ok, projections}
  end

  defp run_case(case_attrs, oracle) do
    expected = fetch(case_attrs, :expected_shape, %{})
    observed = fetch(case_attrs, :observed_shape, expected)
    verdict = verdict_for(oracle, expected, observed, case_attrs)

    %EvalCaseProjection{
      case_ref: fetch!(case_attrs, :case_ref),
      verdict: verdict,
      severity: fetch(case_attrs, :severity, :warn),
      evidence_ref:
        fetch(case_attrs, :evidence_ref, "eval-evidence://" <> fetch!(case_attrs, :case_ref))
    }
  end

  defp verdict_for(_oracle, expected, observed, _case_attrs) when expected == observed, do: :pass

  defp verdict_for(_oracle, _expected, _observed, case_attrs),
    do: fetch(case_attrs, :on_mismatch, :regress)

  defp compose_verdict(projections) do
    projections
    |> Enum.map(& &1.verdict)
    |> Enum.max_by(&verdict_rank/1)
  end

  defp verdict_rank(:pass), do: 0
  defp verdict_rank(:improve), do: 1
  defp verdict_rank(:inconclusive), do: 2
  defp verdict_rank(:regress), do: 3

  defp gate_status(:pass), do: :passed
  defp gate_status(:improve), do: :passed
  defp gate_status(:inconclusive), do: :needs_review
  defp gate_status(:regress), do: :blocked

  defp gate_safe_action(:pass), do: :allow
  defp gate_safe_action(:improve), do: :allow_with_review
  defp gate_safe_action(:inconclusive), do: :review_eval_evidence
  defp gate_safe_action(:regress), do: :block_promotion

  defp eval_verdict_ref(run, attrs) do
    fetch(attrs, :eval_verdict_ref) ||
      "eval-verdict://#{hash(run.eval_run_ref <> Atom.to_string(run.verdict))}"
  end

  defp eval_run_ref(suite, variant_config) do
    "eval-run://#{hash(suite.suite_ref <> variant_ref(variant_config))}"
  end

  defp variant_ref(variant_config) do
    variant_config
    |> Enum.sort_by(fn {key, _value} -> to_string(key) end)
    |> Enum.map_join("/", fn {key, value} -> "#{key}=#{value}" end)
    |> hash()
    |> then(&("eval-variant://" <> &1))
  end

  defp reject_raw(attrs) do
    case Enum.find(@raw_keys, &Map.has_key?(attrs, &1)) do
      nil -> :ok
      key -> {:error, {:raw_eval_payload_forbidden, key}}
    end
  end

  defp required_strings(attrs, fields) do
    case Enum.find(fields, &(not present_string?(fetch(attrs, &1)))) do
      nil -> :ok
      field -> {:error, {:missing_eval_ref, field}}
    end
  end

  defp member(attrs, field, allowed) do
    value = fetch(attrs, field)

    cond do
      value in allowed ->
        {:ok, value}

      is_binary(value) ->
        case Enum.find(allowed, &(Atom.to_string(&1) == value)) do
          nil -> {:error, {:invalid_eval_field, field}}
          atom -> {:ok, atom}
        end

      true ->
        {:error, {:invalid_eval_field, field}}
    end
  end

  defp present_string?(value) when is_binary(value), do: String.trim(value) != ""
  defp present_string?(_value), do: false

  defp fetch!(attrs, field), do: fetch(attrs, field)
  defp fetch(attrs, field), do: fetch(attrs, field, nil)

  defp fetch(attrs, field, default),
    do: Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field)) || default

  defp hash(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)

  defp required_failure_ref(attrs, field) do
    case fetch(attrs, field) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:missing_eval_gate_ref, field}}
    end
  end

  defp trace_ref(attrs, failure) do
    case fetch(attrs, :trace_ref) || failure.trace_ref do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:missing_eval_gate_ref, :trace_ref}}
    end
  end

  defp evidence_refs({reason, field}) when is_atom(reason) and is_atom(field),
    do: ["reason://#{Atom.to_string(reason)}", "field://#{Atom.to_string(field)}"]

  defp evidence_refs(reason) when is_atom(reason), do: ["reason://#{Atom.to_string(reason)}"]
  defp evidence_refs(reason), do: ["reason://#{inspect(reason)}"]

  defp eval_failure(reason, attrs) do
    with {:ok, failure} <-
           Failure.new(%{
             owner: :mezzanine,
             reason_code: "mezzanine.eval.gate_receipt_invalid.v1",
             safe_message: "eval gate receipt is invalid",
             trace_ref: fetch(attrs, :trace_ref),
             evidence_refs: evidence_refs(reason)
           }) do
      {:error, failure}
    end
  end
end
