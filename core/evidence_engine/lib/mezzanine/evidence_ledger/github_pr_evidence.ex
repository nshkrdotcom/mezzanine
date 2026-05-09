defmodule Mezzanine.EvidenceLedger.GitHubPrEvidence do
  @moduledoc """
  Materializes public-safe GitHub PR evidence from governed lower dispatches.

  The inputs are Jido/IntegrationBridge dispatch results that already carry a
  governed lower envelope and receipt. This module reduces them to product-safe
  evidence metadata without exposing provider tokens or raw SDK responses.
  """

  alias Mezzanine.EvidenceLedger.Store

  @evidence_kind "github_pr"

  @spec materialize(map() | [map()], map() | keyword()) :: {:ok, map()} | {:error, term()}
  def materialize(dispatch_or_dispatches, attrs \\ %{})

  def materialize(dispatches, attrs) when is_list(dispatches) do
    attrs = normalize_attrs(attrs)
    dispatches = Enum.filter(dispatches, &is_map/1)

    with :ok <- require_dispatches(dispatches),
         {:ok, pr} <- primary_pull_request(dispatches) do
      {:ok, evidence_record(pr, dispatches, attrs)}
    end
  end

  def materialize(%{} = dispatch, attrs), do: materialize([dispatch], attrs)

  @spec collect(map() | [map()], map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def collect(dispatch_or_dispatches, attrs \\ %{}, opts \\ []) when is_list(opts) do
    with {:ok, evidence} <- materialize(dispatch_or_dispatches, attrs) do
      Store.put_record(evidence, opts)
    end
  end

  defp evidence_record(pr, dispatches, attrs) do
    operations = Enum.map(dispatches, &operation_receipt/1)
    evidence_ref = evidence_ref(pr, attrs)
    content_ref = pr_content_ref(pr)

    %{
      id: evidence_ref,
      installation_id: required_string(attrs, :installation_id),
      subject_id: required_string(attrs, :subject_id),
      execution_id: required_string(attrs, :execution_id),
      evidence_kind: @evidence_kind,
      collector_ref: Map.get(attrs, :collector_ref, "github_pr_ref"),
      content_ref: content_ref,
      status: Map.get(attrs, :status, "collected"),
      trace_id: trace_id(attrs, operations),
      causation_id: required_string(attrs, :causation_id),
      metadata:
        %{
          provider: "github",
          repo: value(pr, :repo),
          pull_number: value(pr, :pull_number),
          html_url: value(pr, :html_url),
          title: value(pr, :title),
          state: value(pr, :state),
          draft: value(pr, :draft),
          merged: value(pr, :merged),
          mergeable: value(pr, :mergeable),
          head: ref_summary(value(pr, :head)),
          base: ref_summary(value(pr, :base)),
          evidence_ref: evidence_ref,
          content_ref: content_ref,
          operations: operations,
          capability_negotiation_receipts: capability_negotiation_receipts(operations),
          authority_refs: unique_present(operations, :authority_ref),
          connector_manifest_refs: unique_present(operations, :connector_manifest_ref),
          lower_receipt_refs: unique_present(operations, :lower_receipt_ref),
          artifact_refs: dispatches |> Enum.flat_map(&artifact_refs/1) |> Enum.uniq(),
          feedback: feedback_summary(dispatches),
          status: status_summary(dispatches),
          cleanup_policy: cleanup_policy(attrs)
        }
        |> compact()
    }
    |> compact()
  end

  defp operation_receipt(dispatch) do
    envelope =
      Map.get(dispatch, :governed_lower_envelope) || Map.get(dispatch, "governed_lower_envelope")

    lower_receipt =
      Map.get(dispatch, :governed_lower_receipt) || Map.get(dispatch, "governed_lower_receipt")

    %{
      capability_id: field_value(envelope, :capability_id) || Map.get(dispatch, :capability),
      status: field_value(lower_receipt, :status) || Map.get(dispatch, :status, :succeeded),
      lower_runtime_kind: lower_runtime_kind(field_value(envelope, :lower_runtime_kind)),
      lower_request_ref: field_value(envelope, :lower_request_ref),
      lower_receipt_ref: field_value(lower_receipt, :lower_receipt_ref),
      authority_ref: field_value(envelope, :authority_ref),
      authority_decision_hash: field_value(envelope, :authority_decision_hash),
      connector_manifest_ref: field_value(envelope, :connector_manifest_ref),
      connector_manifest_hash: field_value(envelope, :connector_manifest_hash),
      capability_negotiation_ref: field_value(envelope, :capability_negotiation_ref),
      provider_response_ref: dispatch |> artifact_refs() |> List.first(),
      trace_id: field_value(envelope, :trace_id)
    }
    |> compact()
  end

  defp capability_negotiation_receipts(operations) do
    Enum.map(operations, fn operation ->
      %{
        capability_id: Map.get(operation, :capability_id),
        capability_negotiation_ref: Map.get(operation, :capability_negotiation_ref),
        connector_manifest_ref: Map.get(operation, :connector_manifest_ref),
        authority_ref: Map.get(operation, :authority_ref),
        status: Map.get(operation, :status)
      }
      |> compact()
    end)
  end

  defp primary_pull_request(dispatches) do
    dispatches
    |> Enum.find_value(&pull_request_from_dispatch/1)
    |> case do
      %{} = pr -> {:ok, pr}
      _missing -> {:error, :missing_github_pr_evidence}
    end
  end

  defp pull_request_from_dispatch(dispatch) do
    output = output(dispatch)

    cond do
      pr_summary?(output) ->
        output

      pr_summary?(value(output, :pull_request)) ->
        value(output, :pull_request)

      match?([%{} | _], value(output, :pull_requests)) ->
        output |> value(:pull_requests) |> List.first()

      true ->
        nil
    end
  end

  defp pr_summary?(%{} = value) do
    present?(value(value, :repo)) and present?(value(value, :pull_number)) and
      present?(value(value, :html_url))
  end

  defp pr_summary?(_value), do: false

  defp feedback_summary(dispatches) do
    reviews = dispatches |> Enum.flat_map(&reviews_from_dispatch/1)
    comments = dispatches |> Enum.flat_map(&review_comments_from_dispatch/1)

    %{
      review_count: length(reviews),
      review_comment_count: length(comments),
      review_states: reviews |> Enum.map(&value(&1, :state)) |> frequencies(),
      unresolved_comment_paths:
        comments |> Enum.map(&value(&1, :path)) |> Enum.reject(&is_nil/1) |> Enum.uniq(),
      rework_required?: rework_required?(reviews, comments)
    }
  end

  defp status_summary(dispatches) do
    outputs = Enum.map(dispatches, &output/1)

    combined =
      Enum.find(outputs, &(present?(value(&1, :state)) and present?(value(&1, :statuses))))

    checks = outputs |> Enum.flat_map(&(value(&1, :check_runs) || [])) |> Enum.filter(&is_map/1)

    %{
      combined_state: value(combined || %{}, :state),
      status_count: combined |> value(:statuses) |> List.wrap() |> Enum.count(),
      check_run_count: length(checks),
      check_run_statuses: checks |> Enum.map(&value(&1, :status)) |> frequencies(),
      check_run_conclusions: checks |> Enum.map(&value(&1, :conclusion)) |> frequencies()
    }
    |> compact()
  end

  defp cleanup_policy(attrs) do
    Map.get(attrs, :cleanup_policy) ||
      %{
        branch_cleanup: "operator_handoff",
        governed_operations: ["github.git.ref.delete"],
        reason: "Delete branches only after merge/close policy approval."
      }
  end

  defp reviews_from_dispatch(dispatch) do
    output = output(dispatch)

    cond do
      is_list(value(output, :reviews)) -> value(output, :reviews)
      is_map(value(output, :review)) -> [value(output, :review)]
      true -> []
    end
  end

  defp review_comments_from_dispatch(dispatch) do
    output = output(dispatch)

    cond do
      is_list(value(output, :comments)) -> value(output, :comments)
      is_map(value(output, :comment)) -> [value(output, :comment)]
      true -> []
    end
  end

  defp rework_required?(reviews, comments) do
    Enum.any?(reviews, &(value(&1, :state) in ["CHANGES_REQUESTED", "changes_requested"])) or
      comments != []
  end

  defp evidence_ref(pr, attrs) do
    Map.get(attrs, :evidence_ref) ||
      "evidence://github-pr/#{value(pr, :repo)}/#{value(pr, :pull_number)}/#{digest([value(pr, :html_url), value(pr, :head)])}"
  end

  defp pr_content_ref(pr), do: "github-pr://#{value(pr, :repo)}/#{value(pr, :pull_number)}"

  defp trace_id(attrs, operations) do
    Map.get(attrs, :trace_id) ||
      operations |> Enum.find_value(&Map.get(&1, :trace_id)) ||
      "trace://github-pr/#{digest(operations)}"
  end

  defp ref_summary(%{} = ref) do
    %{
      ref: value(ref, :ref),
      sha: value(ref, :sha),
      repo: value(ref, :repo)
    }
    |> compact()
  end

  defp ref_summary(_ref), do: nil

  defp artifact_refs(dispatch) do
    dispatch
    |> value(:artifact_refs)
    |> List.wrap()
    |> Enum.filter(&present?/1)
  end

  defp output(dispatch), do: value(dispatch, :output) || %{}

  defp unique_present(operations, key) do
    operations
    |> Enum.map(&Map.get(&1, key))
    |> Enum.filter(&present?/1)
    |> Enum.uniq()
  end

  defp frequencies(values) do
    values
    |> Enum.reject(&is_nil/1)
    |> Enum.frequencies()
  end

  defp require_dispatches([]), do: {:error, :missing_github_dispatches}
  defp require_dispatches(_dispatches), do: :ok

  defp lower_runtime_kind(value) when is_atom(value), do: Atom.to_string(value)
  defp lower_runtime_kind(value), do: value

  defp required_string(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))

  defp field_value(nil, _key), do: nil
  defp field_value(%_{} = struct, key), do: Map.get(struct, key)
  defp field_value(%{} = map, key), do: value(map, key)

  defp value(%_{} = struct, key), do: struct |> Map.from_struct() |> value(key)
  defp value(%{} = map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
  defp value(_map, _key), do: nil

  defp present?(value) when value in [nil, "", []], do: false
  defp present?(_value), do: true

  defp compact(map) do
    map
    |> Enum.reject(fn {_key, value} -> value in [nil, "", [], %{}] end)
    |> Map.new()
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(%{} = attrs), do: Map.new(attrs)

  defp digest(value) do
    value
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> binary_part(0, 24)
  end
end
