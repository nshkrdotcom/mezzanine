defmodule Mezzanine.IntegrationBridge.GitHubPrEvidenceRuntime do
  @moduledoc """
  Lower-owned GitHub PR evidence runtime for product live examples.

  This module installs/redeems a GitHub credential through Jido Integration and
  reads PR, review, status, and check evidence through the governed
  `GitHubPrDispatcher`. It never creates, updates, or cleans up provider state.
  """

  alias Jido.Integration.V2
  alias Jido.Integration.V2.Connectors.GitHub
  alias Jido.Integration.V2.Connectors.GitHub.InstallBinding
  alias Mezzanine.EvidenceLedger.GitHubPrEvidence
  alias Mezzanine.IntegrationBridge
  alias Mezzanine.IntegrationBridge.AuthorizedInvocation

  @connector_id "github"
  @capability_ids [
    "github.pr.fetch",
    "github.pr.reviews.list",
    "github.pr.review_comments.list",
    "github.commit.statuses.get_combined",
    "github.check_runs.list_for_ref"
  ]
  @discovery_capability_id "github.pr.list"
  @api_tool_ids %{
    "github.pr.list" => "github.api.pr.list",
    "github.pr.fetch" => "github.api.pr.fetch",
    "github.pr.reviews.list" => "github.api.pr.reviews.list",
    "github.pr.review_comments.list" => "github.api.pr.review_comments.list",
    "github.commit.statuses.get_combined" => "github.api.commit.statuses.get_combined",
    "github.check_runs.list_for_ref" => "github.api.check_runs.list_for_ref"
  }
  @runtime_apps [
    :jido_integration_v2_auth,
    :jido_integration_v2_control_plane,
    :jido_integration_v2_github
  ]

  @spec fetch(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def fetch(attrs, opts \\ [])

  def fetch(attrs, opts) when is_list(opts) do
    attrs = normalize(attrs)

    with :ok <- reject_hidden_write_setup(attrs),
         :ok <- maybe_start_runtime(opts),
         :ok <- maybe_register_connector(opts),
         {:ok, repo} <- repo(attrs),
         {:ok, connection_id} <- connection_id(attrs, opts),
         {:ok, pull_number} <- pull_number(attrs, repo, connection_id, opts),
         invocation <- authorized_invocation(attrs, @capability_ids),
         dispatch_opts <- dispatch_opts(attrs, connection_id, opts, @capability_ids),
         {:ok, pr_dispatch} <-
           IntegrationBridge.fetch_github_pr(
             invocation,
             %{repo: repo, pull_number: pull_number},
             dispatch_opts
           ),
         {:ok, head_sha} <- head_sha(attrs, pr_dispatch),
         {:ok, %{github_feedback_sweep: sweep}} <-
           IntegrationBridge.sweep_github_pr_feedback(
             invocation,
             %{repo: repo, pull_number: pull_number, ref: head_sha},
             dispatch_opts
           ),
         dispatches <- [pr_dispatch | Map.fetch!(sweep, :dispatches)],
         {:ok, evidence} <- evidence_record(dispatches, attrs, opts) do
      receipt_attrs(evidence, dispatches, attrs, head_sha)
    end
  end

  def fetch(_attrs, _opts), do: {:error, :invalid_github_pr_evidence_runtime_opts}

  defp reject_hidden_write_setup(attrs) do
    if truthy?(map_value(attrs, :setup_fixture?)) or truthy?(map_value(attrs, :write_mode?)) do
      {:error, :github_evidence_write_fixture_requires_separate_command}
    else
      :ok
    end
  end

  defp maybe_start_runtime(opts) do
    case Keyword.get(opts, :start_runtime?, true) do
      true -> start_runtime_apps()
      _skip -> :ok
    end
  end

  defp start_runtime_apps do
    Enum.reduce_while(@runtime_apps, :ok, &start_runtime_app/2)
  end

  defp start_runtime_app(app, :ok) do
    case Application.ensure_all_started(app) do
      {:ok, _started} -> {:cont, :ok}
      {:error, {failed_app, reason}} -> {:halt, {:error, {failed_app, reason}}}
    end
  end

  defp maybe_register_connector(opts) do
    if Keyword.get(opts, :register_connector?, true) do
      V2.register_connector(GitHub)
    else
      :ok
    end
  end

  defp connection_id(attrs, opts) do
    case Keyword.get(opts, :connection_id) || map_value(attrs, :connection_id) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> install_connection(attrs, opts)
    end
  end

  defp install_connection(attrs, opts) do
    with {:ok, access_token} <- access_token(attrs, opts) do
      start_install_fun = Keyword.get(opts, :start_install_fun, &V2.start_install/3)
      complete_install_fun = Keyword.get(opts, :complete_install_fun, &V2.complete_install/2)
      auth = GitHub.manifest().auth
      binding = InstallBinding.from_personal_access_token(access_token)
      now = Keyword.get(opts, :now, DateTime.utc_now())
      tenant_id = required_string(attrs, :tenant_id)
      actor_id = actor_id(attrs)
      subject = subject(attrs)

      with {:ok, %{install: install, connection: installing_connection}} <-
             start_install_fun.(@connector_id, tenant_id, %{
               actor_id: actor_id,
               auth_type: auth.auth_type,
               profile_id: auth.default_profile,
               subject: subject,
               requested_scopes: auth.requested_scopes,
               metadata: %{proof: "github evidence runtime read path"},
               now: now
             }),
           {:ok, %{connection: connection}} <-
             complete_install_fun.(
               install.install_id,
               InstallBinding.complete_install_attrs(subject, auth.requested_scopes, binding,
                 now: now
               )
             ) do
        {:ok,
         map_value(connection, :connection_id) || map_value(installing_connection, :connection_id)}
      end
    end
  end

  defp access_token(attrs, opts) do
    token =
      Keyword.get(opts, :access_token) ||
        map_value(attrs, :access_token) ||
        configured_access_token()

    if is_binary(token) and String.trim(token) != "" do
      {:ok, String.trim(token)}
    else
      {:error, :missing_github_access_token}
    end
  end

  defp configured_access_token do
    :mezzanine_integration_bridge
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:access_token)
  end

  defp pull_number(attrs, repo, connection_id, opts) do
    case positive_integer(map_value(attrs, :pull_number)) do
      {:ok, pull_number} -> {:ok, pull_number}
      :error -> discover_pull_number(attrs, repo, connection_id, opts)
    end
  end

  defp discover_pull_number(attrs, repo, connection_id, opts) do
    capabilities = [@discovery_capability_id | @capability_ids]
    invocation = authorized_invocation(attrs, capabilities)
    dispatch_opts = dispatch_opts(attrs, connection_id, opts, capabilities)

    with {:ok, result} <-
           IntegrationBridge.list_github_prs(
             invocation,
             %{repo: repo, state: "all", per_page: 1, page: 1},
             dispatch_opts
           ),
         pull_requests when is_list(pull_requests) <-
           result |> output() |> map_value(:pull_requests),
         {:ok, pull_number} <- first_pull_number(pull_requests) do
      {:ok, pull_number}
    else
      {:error, reason} -> {:error, reason}
      _missing -> {:error, :missing_live_github_pr}
    end
  end

  defp first_pull_number([pull_request | _rest]),
    do: positive_integer(map_value(pull_request, :pull_number))

  defp first_pull_number(_pull_requests), do: {:error, :missing_live_github_pr}

  defp head_sha(attrs, dispatch) do
    case string_value(map_value(attrs, :ref)) ||
           dispatch |> output() |> map_value(:head) |> map_value(:sha) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, :missing_github_pr_head_sha}
    end
  end

  defp evidence_record(dispatches, attrs, opts) do
    evidence_attrs = %{
      installation_id: required_string(attrs, :installation_id),
      subject_id: required_string(attrs, :subject_id),
      execution_id: required_string(attrs, :execution_id),
      trace_id: required_string(attrs, :trace_id),
      causation_id: map_value(attrs, :causation_id) || required_string(attrs, :execution_id),
      collector_ref: "github_pr_evidence_runtime"
    }

    if Keyword.get(opts, :collect?, true) do
      GitHubPrEvidence.collect(dispatches, evidence_attrs, opts)
    else
      GitHubPrEvidence.materialize(dispatches, evidence_attrs)
    end
  end

  defp receipt_attrs(evidence, dispatches, _attrs, head_sha) do
    metadata = map_value(evidence, :metadata) || %{}
    operations = map_value(metadata, :operations) || []
    receipt_refs = receipt_refs(metadata, operations)
    lower_request_ref = first_present(receipt_refs.lower_request_refs)
    lower_receipt_ref = first_present(receipt_refs.lower_receipt_refs)

    {:ok,
     %{
       effect_ref: "live-effect://github/pr-evidence/#{ref_suffix(map_value(evidence, :id))}",
       provider: "github",
       effect: "github_pr_evidence",
       status: :receipt_recorded,
       capability_ids: Enum.map(operations, &map_value(&1, :capability_id)),
       repo: map_value(metadata, :repo),
       pull_number: map_value(metadata, :pull_number),
       head_sha: head_sha,
       evidence_ref: map_value(evidence, :id),
       credential_present?: true,
       credential_redeemed?: true,
       provider_request_sent?: true,
       provider_response_received?: true,
       receipt_recorded?: true,
       product_readback_confirmed?: true,
       fixture_setup_required?: false,
       write_operations: [],
       provider_ids: provider_ids(dispatches, head_sha),
       provider_refs: provider_refs(evidence, metadata),
       counts: counts(metadata),
       receipt_refs: receipt_refs,
       operation_receipts: operations,
       metadata: %{
         "evidence_kind" => map_value(evidence, :evidence_kind),
         "collector_ref" => map_value(evidence, :collector_ref),
         "cleanup_policy" => map_value(metadata, :cleanup_policy),
         "lower_request_ref" => lower_request_ref,
         "lower_receipt_ref" => lower_receipt_ref
       }
     }}
  end

  defp provider_ids(dispatches, head_sha) do
    %{
      pull_request:
        dispatches |> pull_request_output() |> map_value(:pull_number) |> string_value(),
      reviews: dispatches |> list_output(:reviews) |> ids(:review_id),
      review_comments: dispatches |> list_output(:comments) |> ids(:comment_id),
      check_runs: dispatches |> list_output(:check_runs) |> ids(:check_run_id),
      combined_status_ref: head_sha
    }
    |> compact()
  end

  defp provider_refs(evidence, metadata) do
    %{
      pull_request: map_value(metadata, :html_url),
      content_ref: map_value(evidence, :content_ref)
    }
    |> compact()
  end

  defp counts(metadata) do
    feedback = map_value(metadata, :feedback) || %{}
    status = map_value(metadata, :status) || %{}

    %{
      review_count: map_value(feedback, :review_count),
      review_comment_count: map_value(feedback, :review_comment_count),
      status_count: map_value(status, :status_count),
      check_run_count: map_value(status, :check_run_count)
    }
    |> compact()
  end

  defp receipt_refs(metadata, operations) do
    %{
      evidence_ref: map_value(metadata, :evidence_ref),
      lower_request_refs:
        operations |> Enum.map(&map_value(&1, :lower_request_ref)) |> present_uniq(),
      lower_receipt_refs:
        operations |> Enum.map(&map_value(&1, :lower_receipt_ref)) |> present_uniq(),
      artifact_refs: map_value(metadata, :artifact_refs) || []
    }
    |> compact()
  end

  defp authorized_invocation(attrs, allowed_operations) do
    AuthorizedInvocation.new!(%{
      tenant_id: required_string(attrs, :tenant_id),
      installation_id: required_string(attrs, :installation_id),
      subject_id: required_string(attrs, :subject_id),
      execution_id: required_string(attrs, :execution_id),
      trace_id: required_string(attrs, :trace_id),
      idempotency_key:
        map_value(attrs, :idempotency_key) ||
          "github-pr-evidence:#{ref_suffix(required_string(attrs, :trace_id))}",
      submission_dedupe_key:
        map_value(attrs, :submission_dedupe_key) ||
          "github-pr-evidence:#{ref_suffix(required_string(attrs, :execution_id))}",
      invocation_request: invocation_request(attrs, allowed_operations)
    })
  end

  defp invocation_request(attrs, allowed_operations) do
    tenant_id = required_string(attrs, :tenant_id)
    trace_id = required_string(attrs, :trace_id)
    actor_id = actor_id(attrs)
    installation_id = required_string(attrs, :installation_id)
    subject_id = required_string(attrs, :subject_id)
    execution_id = required_string(attrs, :execution_id)
    decision_hash = sha256([trace_id, execution_id, allowed_operations])

    %{
      schema_version: 2,
      invocation_request_id: "invoke-github-pr-evidence-#{ref_suffix(execution_id)}",
      request_id: "request-github-pr-evidence-#{ref_suffix(trace_id)}",
      session_id: "session-github-pr-evidence-#{ref_suffix(execution_id)}",
      tenant_id: tenant_id,
      trace_id: trace_id,
      actor_id: actor_id,
      target_id: "target-github-pr-evidence",
      target_kind: "runtime_target",
      selected_step_id: "github-pr-evidence",
      allowed_operations: allowed_operations,
      authority_packet: %{
        contract_version: "v1",
        decision_id: "github-pr-evidence-#{ref_suffix(execution_id)}",
        tenant_id: tenant_id,
        request_id: "request-github-pr-evidence-#{ref_suffix(trace_id)}",
        policy_version: "github-pr-evidence-live-v1",
        boundary_class: "workspace_session",
        trust_profile: "baseline",
        approval_profile: "standard",
        egress_profile: "restricted",
        workspace_profile: "workspace",
        resource_profile: "standard",
        decision_hash: decision_hash,
        extensions: %{"citadel" => %{}}
      },
      boundary_intent: %{},
      topology_intent: %{},
      execution_governance: %{
        contract_version: "v1",
        execution_governance_id: "github-pr-evidence-governance-#{ref_suffix(execution_id)}",
        authority_ref: %{"decision_id" => "github-pr-evidence-#{ref_suffix(execution_id)}"},
        sandbox: %{
          "level" => "strict",
          "egress" => "restricted",
          "approvals" => "manual",
          "allowed_tools" => api_tool_ids(allowed_operations)
        },
        boundary: %{},
        topology: %{},
        workspace: %{},
        resources: %{},
        placement: %{},
        operations: %{"allowed_operations" => allowed_operations},
        extensions: %{"citadel" => %{}}
      },
      extensions: %{
        "citadel" => %{
          "execution_envelope" => %{
            "installation_id" => installation_id,
            "installation_revision" => 1,
            "subject_id" => subject_id,
            "execution_id" => execution_id,
            "submission_dedupe_key" => "github-pr-evidence:#{ref_suffix(execution_id)}"
          }
        }
      }
    }
  end

  defp dispatch_opts(attrs, connection_id, opts, allowed_operations) do
    api_tool_ids = api_tool_ids(allowed_operations)

    [
      invoke_fun: Keyword.get(opts, :invoke_fun, &V2.invoke/3),
      invoke_opts:
        [
          connection_id: connection_id,
          actor_id: actor_id(attrs),
          tenant_id: required_string(attrs, :tenant_id),
          trace_id: required_string(attrs, :trace_id),
          environment: Keyword.get(opts, :environment, :prod),
          allowed_operations: allowed_operations,
          sandbox: %{
            level: :strict,
            egress: :restricted,
            approvals: :manual,
            allowed_tools: api_tool_ids
          }
        ]
        |> put_present(:github_client, Keyword.get(opts, :github_client))
    ]
  end

  defp api_tool_ids(allowed_operations) do
    allowed_operations
    |> Enum.map(&Map.fetch!(@api_tool_ids, &1))
    |> Enum.uniq()
  end

  defp repo(attrs) do
    case string_value(map_value(attrs, :repo)) do
      value when is_binary(value) -> {:ok, value}
      _missing -> {:error, :missing_github_repo}
    end
  end

  defp output(dispatch), do: map_value(dispatch, :output) || %{}

  defp pull_request_output(dispatches) do
    Enum.find_value(dispatches, fn dispatch ->
      output = output(dispatch)

      cond do
        map_value(output, :pull_number) -> output
        is_map(map_value(output, :pull_request)) -> map_value(output, :pull_request)
        true -> nil
      end
    end) || %{}
  end

  defp list_output(dispatches, key) do
    dispatches
    |> Enum.flat_map(fn dispatch -> output(dispatch) |> map_value(key) |> List.wrap() end)
    |> Enum.filter(&is_map/1)
  end

  defp ids(values, key) do
    values
    |> Enum.map(&(map_value(&1, key) |> string_value()))
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp actor_id(attrs),
    do: string_value(map_value(attrs, :actor_id)) || "actor://mezzanine/github-pr-evidence"

  defp subject(attrs),
    do: string_value(map_value(attrs, :credential_subject)) || "github-pr-evidence-live"

  defp required_string(attrs, key) do
    case string_value(map_value(attrs, key)) do
      value when is_binary(value) -> value
      _missing -> raise ArgumentError, "missing required GitHub PR evidence field #{inspect(key)}"
    end
  end

  defp positive_integer(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp positive_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer > 0 -> {:ok, integer}
      _other -> :error
    end
  end

  defp positive_integer(_value), do: :error

  defp put_present(keyword, _key, nil), do: keyword
  defp put_present(keyword, key, value), do: Keyword.put(keyword, key, value)

  defp present_uniq(values) do
    values
    |> Enum.filter(&present?/1)
    |> Enum.uniq()
  end

  defp first_present(values), do: Enum.find(List.wrap(values), &present?/1)

  defp compact(map), do: Map.reject(map, fn {_key, value} -> value in [nil, "", [], %{}] end)

  defp present?(value), do: value not in [nil, "", []]

  defp normalize(%_{} = attrs), do: attrs |> Map.from_struct() |> normalize()
  defp normalize(attrs) when is_map(attrs), do: attrs
  defp normalize(attrs) when is_list(attrs), do: Map.new(attrs)

  defp map_value(%_{} = struct, key), do: struct |> Map.from_struct() |> map_value(key)
  defp map_value(%{} = map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
  defp map_value(_value, _key), do: nil

  defp string_value(value) when is_binary(value) and value != "", do: value
  defp string_value(value) when is_atom(value), do: Atom.to_string(value)
  defp string_value(value) when is_integer(value), do: Integer.to_string(value)
  defp string_value(_value), do: nil

  defp truthy?(value), do: value in [true, "true", 1, "1"]

  defp ref_suffix(ref) when is_binary(ref) do
    ref
    |> :binary.bin_to_list()
    |> Enum.reduce({[], false}, &ascii_alnum_dash_byte/2)
    |> elem(0)
    |> Enum.reverse()
    |> List.to_string()
    |> String.trim("-")
  end

  defp ref_suffix(ref), do: ref |> to_string() |> ref_suffix()

  defp ascii_alnum_dash_byte(byte, {chars, _previous_dash?}) when byte in ?A..?Z,
    do: {[byte | chars], false}

  defp ascii_alnum_dash_byte(byte, {chars, _previous_dash?}) when byte in ?a..?z,
    do: {[byte | chars], false}

  defp ascii_alnum_dash_byte(byte, {chars, _previous_dash?}) when byte in ?0..?9,
    do: {[byte | chars], false}

  defp ascii_alnum_dash_byte(_byte, {chars, true}), do: {chars, true}
  defp ascii_alnum_dash_byte(_byte, {chars, false}), do: {[?- | chars], true}

  defp sha256(term) do
    term
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
