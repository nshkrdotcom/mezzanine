defmodule Mezzanine.IntegrationBridge.ProviderAdapters.GitHub.PrBranchCleanupRuntime do
  @moduledoc """
  Lower-owned GitHub PR branch cleanup runtime.

  The runtime discovers open pull requests for a branch and closes each match
  through governed GitHub connector operations. It is generic lower
  infrastructure and intentionally contains no product-specific naming.
  """

  alias Jido.Integration.V2
  alias Jido.Integration.V2.Connectors.GitHub
  alias Jido.Integration.V2.Connectors.GitHub.InstallBinding
  alias Mezzanine.IntegrationBridge.AuthorizedInvocation
  alias Mezzanine.IntegrationBridge.ProviderAdapters.GitHub.PrDispatcher
  alias Mezzanine.IntegrationBridge.ProviderAuthorityAdmission

  @connector_id "github"
  @capability_ids ["github.pr.list", "github.comment.create", "github.pr.update"]
  @api_tool_ids %{
    "github.pr.list" => "github.api.pr.list",
    "github.comment.create" => "github.api.comment.create",
    "github.pr.update" => "github.api.pr.update"
  }
  @runtime_apps [
    :jido_integration_v2_auth,
    :jido_integration_v2_control_plane,
    :jido_integration_v2_github
  ]

  @spec cleanup(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def cleanup(attrs, opts \\ [])

  def cleanup(attrs, opts) when is_list(opts) do
    attrs = normalize(attrs)

    with :ok <- confirmed?(attrs, opts),
         :ok <- maybe_start_runtime(opts),
         :ok <- maybe_register_connector(opts),
         {:ok, repo} <- repo(attrs),
         {:ok, branch} <- branch(attrs),
         {:ok, connection_id} <- connection_id(attrs, opts),
         allowed_operations <- allowed_operations(attrs, opts),
         invocation <- authorized_invocation(attrs, allowed_operations),
         dispatch_opts <- dispatch_opts(attrs, connection_id, opts, allowed_operations),
         {:ok, list_dispatch} <-
           PrDispatcher.list_prs(invocation, list_attrs(repo, branch), dispatch_opts),
         pull_requests <- matching_pull_requests(list_dispatch, attrs, branch),
         {:ok, close_dispatches} <-
           close_pull_requests(invocation, repo, branch, pull_requests, attrs, dispatch_opts) do
      receipt_attrs([list_dispatch | close_dispatches], attrs, repo, branch, pull_requests)
    end
  end

  def cleanup(_attrs, _opts), do: {:error, :invalid_github_pr_branch_cleanup_runtime_opts}

  defp allowed_operations(attrs, opts) do
    case Keyword.get(opts, :allowed_operations) || map_value(attrs, :allowed_operations) do
      operations when is_list(operations) and operations != [] ->
        Enum.map(operations, &to_string/1)

      _missing ->
        @capability_ids
    end
  end

  defp confirmed?(attrs, opts) do
    if truthy?(map_value(attrs, :confirm_close?)) or Keyword.get(opts, :confirm_close?) == true do
      :ok
    else
      {:error, :github_pr_branch_cleanup_requires_confirmation}
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
               metadata: %{proof: "github pr branch cleanup runtime"},
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

  defp list_attrs(repo, branch),
    do: %{repo: repo, state: "open", head: branch, per_page: 100, page: 1}

  defp matching_pull_requests(list_dispatch, attrs, branch) do
    requested_number = positive_integer(map_value(attrs, :pull_number))
    pull_requests = list_dispatch |> output() |> map_value(:pull_requests) |> List.wrap()

    Enum.filter(pull_requests, fn pull_request ->
      is_map(pull_request) and branch_match?(pull_request, branch) and
        requested_pull_number_match?(pull_request, requested_number)
    end)
  end

  defp requested_pull_number_match?(pull_request, {:ok, number}),
    do: pull_number(pull_request) == number

  defp requested_pull_number_match?(_pull_request, :error), do: true

  defp branch_match?(pull_request, branch) do
    head = map_value(pull_request, :head)

    [
      string_value(map_value(head, :ref)),
      string_value(map_value(head, :label)),
      string_value(map_value(pull_request, :head_ref)),
      string_value(map_value(pull_request, :head_label)),
      string_value(map_value(pull_request, :head))
    ]
    |> Enum.any?(&branch_ref_match?(&1, branch))
  end

  defp branch_ref_match?(nil, _branch), do: false

  defp branch_ref_match?(value, branch),
    do: value == branch or String.ends_with?(value, ":#{branch}")

  defp close_pull_requests(invocation, repo, branch, pull_requests, attrs, dispatch_opts) do
    Enum.reduce_while(pull_requests, {:ok, []}, fn pull_request, {:ok, acc} ->
      number = pull_number(pull_request)

      with {:ok, comment_dispatch} <-
             PrDispatcher.create_comment(
               invocation,
               %{repo: repo, issue_number: number, body: closing_comment(attrs, branch)},
               dispatch_opts
             ),
           {:ok, update_dispatch} <-
             PrDispatcher.update_pr(
               invocation,
               %{repo: repo, pull_number: number, state: "closed"},
               dispatch_opts
             ) do
        {:cont, {:ok, acc ++ [comment_dispatch, update_dispatch]}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp closing_comment(attrs, branch) do
    string_value(map_value(attrs, :closing_comment)) ||
      "Closing because the branch #{branch} is being cleaned up before workspace removal."
  end

  defp receipt_attrs(dispatches, attrs, repo, branch, pull_requests) do
    operations = operation_receipts(dispatches)
    receipt_refs = receipt_refs(dispatches, operations)
    closed_numbers = pull_numbers(pull_requests)
    status = if closed_numbers == [], do: :skipped, else: :receipt_recorded
    authority_handoff = authority_handoff_metadata(dispatches)

    {:ok,
     %{
       effect_ref:
         "live-effect://github/pr-branch-cleanup/#{ref_suffix(required_string(attrs, :trace_id))}",
       provider: "github",
       effect: "github_pr_branch_cleanup",
       status: status,
       capability_ids: Enum.map(operations, &map_value(&1, :capability_id)),
       repo: repo,
       branch: branch,
       pull_numbers: closed_numbers,
       closed_pull_numbers: closed_numbers,
       credential_present?: true,
       credential_redeemed?: true,
       provider_request_sent?: true,
       provider_response_received?: true,
       receipt_recorded?: true,
       product_readback_confirmed?: true,
       write_operations: write_operations(closed_numbers),
       provider_ids: %{pull_requests: Enum.map(closed_numbers, &Integer.to_string/1)},
       provider_refs: %{pull_requests: provider_refs(pull_requests)},
       counts: %{
         matched_count: length(pull_requests),
         closed_count: length(closed_numbers),
         comment_count: length(closed_numbers)
       },
       receipt_refs: receipt_refs,
       operation_receipts: operations,
       metadata:
         %{
           "cleanup_policy" => "close_open_prs_for_branch",
           "lower_request_ref" => first_present(receipt_refs.lower_request_refs),
           "lower_receipt_ref" => first_present(receipt_refs.lower_receipt_refs),
           "authority_handoff" => authority_handoff
         }
         |> compact()
     }}
  end

  defp operation_receipts(dispatches) do
    dispatches
    |> Enum.map(&map_value(&1, :github_operation_receipt))
    |> Enum.filter(&is_map/1)
  end

  defp receipt_refs(dispatches, operations) do
    %{
      lower_request_refs:
        operations |> Enum.map(&map_value(&1, :lower_request_ref)) |> present_uniq(),
      lower_receipt_refs:
        operations |> Enum.map(&map_value(&1, :lower_receipt_ref)) |> present_uniq(),
      artifact_refs:
        dispatches
        |> Enum.flat_map(&(map_value(&1, :artifact_refs) |> List.wrap()))
        |> present_uniq()
    }
    |> compact()
  end

  defp authority_handoff_metadata(dispatches) do
    dispatches
    |> Enum.find_value(fn dispatch ->
      dispatch
      |> map_value(:authority_handoff)
      |> ProviderAuthorityAdmission.metadata()
      |> case do
        metadata when map_size(metadata) > 0 -> metadata
        _empty -> nil
      end
    end)
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

  defp authorized_invocation(attrs, allowed_operations) do
    AuthorizedInvocation.new!(%{
      tenant_id: required_string(attrs, :tenant_id),
      installation_id: required_string(attrs, :installation_id),
      subject_id: required_string(attrs, :subject_id),
      execution_id: required_string(attrs, :execution_id),
      trace_id: required_string(attrs, :trace_id),
      idempotency_key:
        map_value(attrs, :idempotency_key) ||
          "github-pr-branch-cleanup:#{ref_suffix(required_string(attrs, :trace_id))}",
      submission_dedupe_key:
        map_value(attrs, :submission_dedupe_key) ||
          "github-pr-branch-cleanup:#{ref_suffix(required_string(attrs, :execution_id))}",
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
      invocation_request_id: "invoke-github-pr-branch-cleanup-#{ref_suffix(execution_id)}",
      request_id: "request-github-pr-branch-cleanup-#{ref_suffix(trace_id)}",
      session_id: "session-github-pr-branch-cleanup-#{ref_suffix(execution_id)}",
      tenant_id: tenant_id,
      trace_id: trace_id,
      actor_id: actor_id,
      target_id: "target-github-pr-branch-cleanup",
      target_kind: "runtime_target",
      selected_step_id: "github-pr-branch-cleanup",
      allowed_operations: allowed_operations,
      authority_packet: %{
        contract_version: "v1",
        decision_id: "github-pr-branch-cleanup-#{ref_suffix(execution_id)}",
        tenant_id: tenant_id,
        request_id: "request-github-pr-branch-cleanup-#{ref_suffix(trace_id)}",
        policy_version: "github-pr-branch-cleanup-live-v1",
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
        execution_governance_id:
          "github-pr-branch-cleanup-governance-#{ref_suffix(execution_id)}",
        authority_ref: %{"decision_id" => "github-pr-branch-cleanup-#{ref_suffix(execution_id)}"},
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
            "submission_dedupe_key" => "github-pr-branch-cleanup:#{ref_suffix(execution_id)}"
          }
        }
      }
    }
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

  defp branch(attrs) do
    case string_value(map_value(attrs, :branch)) || string_value(map_value(attrs, :head)) do
      value when is_binary(value) -> {:ok, value}
      _missing -> {:error, :missing_github_branch}
    end
  end

  defp output(dispatch), do: map_value(dispatch, :output) || %{}

  defp pull_numbers(pull_requests), do: Enum.map(pull_requests, &pull_number/1)

  defp pull_number(pull_request) do
    case positive_integer(map_value(pull_request, :pull_number)) do
      {:ok, value} -> value
      :error -> map_value(pull_request, :number)
    end
  end

  defp provider_refs(pull_requests) do
    pull_requests
    |> Enum.map(&string_value(map_value(&1, :html_url)))
    |> Enum.reject(&is_nil/1)
  end

  defp write_operations([]), do: []
  defp write_operations(_closed_numbers), do: ["github.comment.create", "github.pr.update"]

  defp actor_id(attrs),
    do: string_value(map_value(attrs, :actor_id)) || "actor://mezzanine/github-pr-branch-cleanup"

  defp subject(attrs),
    do: string_value(map_value(attrs, :credential_subject)) || "github-pr-branch-cleanup-live"

  defp required_string(attrs, key) do
    case string_value(map_value(attrs, key)) do
      value when is_binary(value) ->
        value

      _missing ->
        raise ArgumentError, "missing required GitHub PR branch cleanup field #{inspect(key)}"
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

  defp normalize(%_{} = attrs), do: attrs |> Map.from_struct() |> normalize()
  defp normalize(attrs) when is_map(attrs), do: attrs
  defp normalize(attrs) when is_list(attrs), do: Map.new(attrs)

  defp map_value(%_{} = struct, key), do: struct |> Map.from_struct() |> map_value(key)
  defp map_value(%{} = map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
  defp map_value(_value, _key), do: nil

  defp string_value(nil), do: nil
  defp string_value(value) when is_binary(value) and value != "", do: value
  defp string_value(value) when is_atom(value), do: Atom.to_string(value)
  defp string_value(value) when is_integer(value), do: Integer.to_string(value)
  defp string_value(_value), do: nil

  defp put_present(keyword, _key, nil), do: keyword
  defp put_present(keyword, key, value), do: Keyword.put(keyword, key, value)

  defp present_uniq(values) do
    values
    |> Enum.filter(&present?/1)
    |> Enum.uniq()
  end

  defp first_present(values), do: Enum.find(List.wrap(values), &present?/1)
  defp present?(value), do: value not in [nil, "", []]
  defp compact(map), do: Map.reject(map, fn {_key, value} -> value in [nil, "", [], %{}] end)
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
