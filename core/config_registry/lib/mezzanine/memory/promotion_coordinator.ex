defmodule Mezzanine.Memory.PromotionCoordinator do
  @moduledoc """
  Mezzanine-owned coordinator for promoting shared memory into governed memory.

  The coordinator owns the decision path and produces the governed write
  payload. Storage, review, workflow outbox, attachment, projection, and proof
  emission are supplied as callbacks so product repos do not write governed
  memory directly.
  """

  alias Mezzanine.Audit.MemoryProofToken
  alias Mezzanine.ConfigRegistry.PromotePolicy
  alias Mezzanine.Memory.{MemoryCandidate, MemoryPromotionDecision}

  @segment_size 20
  @signal_name "memory.promotion.decision"
  @signal_version "memory-promotion-decision.v1"
  @workflow_type "memory_promotion"
  @workflow_version "memory-promotion.v1"
  @workflow_input_version "memory-promotion-input.v1"
  @normalizable_keys [
    :candidate_id,
    :decision,
    :decision_id,
    :decision_ref,
    :node_instance_id,
    :node_shortname,
    :reason,
    :review_ref,
    :review_refs,
    :signal_name,
    :signal_version,
    :source_node_ref
  ]
  @key_lookup Map.new(@normalizable_keys, &{Atom.to_string(&1), &1})

  @type callback_opts :: keyword()

  @spec propose_candidate(MemoryCandidate.V1.t() | map() | keyword(), callback_opts()) ::
          {:ok, String.t()} | {:error, term()}
  def propose_candidate(candidate, opts \\ [])

  def propose_candidate(%MemoryCandidate.V1{} = candidate, opts) when is_list(opts) do
    with :ok <- ensure_candidate_can_run(candidate),
         :ok <- claim_candidate(candidate, opts),
         {:ok, policy} <- resolve_policy(candidate, opts),
         {:ok, workflow_plan} <- workflow_plan(candidate, policy, opts),
         {:ok, _workflow_ref} <- call(opts, :workflow_enqueue, [workflow_plan]),
         {:ok, decision} <- resolve_decision(candidate, policy, workflow_plan, opts),
         :ok <- apply_decision(candidate, policy, workflow_plan, decision, opts) do
      {:ok, candidate.candidate_id}
    end
  end

  def propose_candidate(candidate_attrs, opts) when is_list(opts) do
    with {:ok, candidate} <- MemoryCandidate.V1.new(candidate_attrs) do
      propose_candidate(candidate, opts)
    end
  end

  @spec signal_registry() :: [map()]
  def signal_registry do
    [
      %{
        signal_name: @signal_name,
        signal_version: @signal_version,
        signal_effect: "promotion_decision_received",
        workflow_type: @workflow_type,
        terminal?: true
      }
    ]
  end

  @spec registered_signal?(String.t(), String.t()) :: boolean()
  def registered_signal?(signal_name, signal_version) do
    Enum.any?(signal_registry(), fn entry ->
      entry.signal_name == signal_name and entry.signal_version == signal_version
    end)
  end

  @spec validate_workflow_signal(map() | keyword()) :: :ok | {:error, term()}
  def validate_workflow_signal(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)
    required_fields = [:signal_name, :signal_version, :candidate_id, :decision_id]

    case missing_required_strings(attrs, required_fields) do
      [] ->
        signal_name = Map.fetch!(attrs, :signal_name)
        signal_version = Map.fetch!(attrs, :signal_version)

        if registered_signal?(signal_name, signal_version) do
          :ok
        else
          {:error, {:unregistered_signal, signal_name, signal_version}}
        end

      missing ->
        {:error, {:missing_required_fields, missing}}
    end
  end

  def validate_workflow_signal(_attrs), do: {:error, :invalid_workflow_signal}

  defp ensure_candidate_can_run(%MemoryCandidate.V1{quarantined?: true}),
    do: {:error, :quarantined_candidate}

  defp ensure_candidate_can_run(%MemoryCandidate.V1{}), do: :ok

  defp claim_candidate(candidate, opts) do
    context = %{candidate: candidate}

    case call(opts, :claim_candidate, [context]) do
      {:ok, :new} ->
        :ok

      {:ok, %{source_node_ref: same_node}} when same_node == candidate.source_node_ref ->
        :ok

      {:ok, existing} when is_map(existing) ->
        {:error,
         {:duplicate_candidate_from_different_node,
          %{
            candidate_id: candidate.candidate_id,
            existing_source_node_ref:
              Map.get(existing, :source_node_ref) ||
                Map.get(existing, "source_node_ref")
          }}}

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, {:invalid_candidate_claim, other}}
    end
  end

  defp resolve_policy(candidate, opts) do
    with {:ok, policy} <- call(opts, :promote_policy, [%{candidate: candidate}]) do
      PromotePolicy.new(policy)
    end
  end

  defp workflow_plan(candidate, policy, opts) do
    task_queue = promotion_queue(candidate.installation_ref)

    with {:ok, worker_identity} <- worker_identity(task_queue, opts) do
      {:ok,
       %{
         workflow_type: @workflow_type,
         workflow_version: @workflow_version,
         workflow_id: workflow_id(candidate, policy),
         workflow_input_version: @workflow_input_version,
         workflow_input_ref: "memory-promotion-input://#{candidate.candidate_id}",
         task_queue: task_queue,
         worker_identity: worker_identity,
         idempotency_key: idempotency_key(candidate, policy),
         dedupe_scope: "memory-promotion:#{candidate.tenant_ref}:#{candidate.candidate_id}",
         signal_name: @signal_name,
         signal_version: @signal_version,
         args: %{
           tenant_ref: candidate.tenant_ref,
           installation_ref: candidate.installation_ref,
           candidate_id: candidate.candidate_id,
           promotion_policy_ref: policy.policy_id,
           trace_id: candidate.trace_id,
           source_node_ref: candidate.source_node_ref,
           commit_lsn: candidate.commit_lsn,
           commit_hlc: candidate.commit_hlc
         },
         search_attributes: %{
           "phase7.workflow_type" => @workflow_type,
           "phase7.candidate_id" => candidate.candidate_id,
           "phase7.promotion_policy_ref" => policy.policy_id
         }
       }}
    end
  end

  defp worker_identity(task_queue, opts) do
    with {:ok, node_shortname} <- valid_node_shortname(Keyword.get(opts, :node_shortname)),
         {:ok, node_instance_id} <- valid_node_instance_id(Keyword.get(opts, :node_instance_id)),
         {:ok, worker_role} <- valid_worker_role(:promotion_worker),
         {:ok, task_queue_hash} <- valid_task_queue_hash(task_queue) do
      [
        node_shortname,
        binary_part(node_instance_id, 0, 8),
        worker_role,
        task_queue_hash
      ]
      |> Enum.join("/")
      |> ensure_worker_identity_size()
    end
  end

  defp promotion_queue(installation_ref) when is_binary(installation_ref) do
    "mez.promotion.#{hash_segment(installation_ref)}"
  end

  defp hash_segment(typed_ref) when is_binary(typed_ref) do
    :sha256
    |> :crypto.hash(typed_ref)
    |> Base.encode32(case: :lower, padding: false)
    |> binary_part(0, @segment_size)
  end

  defp valid_node_shortname(value) when is_binary(value) and value != "" do
    if node_shortname?(value) do
      {:ok, value}
    else
      {:error, :invalid_node_shortname}
    end
  end

  defp valid_node_shortname(_value), do: {:error, :invalid_node_shortname}

  defp valid_node_instance_id(value) when is_binary(value) and byte_size(value) >= 8,
    do: {:ok, value}

  defp valid_node_instance_id(_value), do: {:error, :invalid_node_instance_id}

  defp valid_worker_role(value) when is_atom(value) do
    value
    |> Atom.to_string()
    |> valid_worker_role()
  end

  defp valid_worker_role(value) when is_binary(value) and value != "" do
    if node_shortname?(value) do
      {:ok, value}
    else
      {:error, :invalid_worker_role}
    end
  end

  defp valid_worker_role(_value), do: {:error, :invalid_worker_role}

  defp valid_task_queue_hash(value) when is_binary(value) and value != "" do
    {:ok, task_queue_hash(value)}
  end

  defp valid_task_queue_hash(_value), do: {:error, :invalid_task_queue}

  defp task_queue_hash(queue) do
    typed_queue_hash(queue) || hash_segment(queue)
  end

  defp ensure_worker_identity_size(identity) when byte_size(identity) <= 96, do: {:ok, identity}
  defp ensure_worker_identity_size(_identity), do: {:error, :worker_identity_too_long}

  defp typed_queue_hash("mez.promotion." <> segment), do: valid_queue_hash_segment(segment)
  defp typed_queue_hash("mez.workflow_runtime." <> segment), do: valid_queue_hash_segment(segment)
  defp typed_queue_hash(_queue), do: nil

  defp valid_queue_hash_segment(segment) do
    if byte_size(segment) == @segment_size and base32lower?(segment), do: segment, else: nil
  end

  defp base32lower?(value) do
    value
    |> :binary.bin_to_list()
    |> Enum.all?(fn byte -> byte in ?a..?z or byte in ?2..?7 end)
  end

  defp node_shortname?(value) do
    value
    |> :binary.bin_to_list()
    |> Enum.all?(fn byte ->
      byte in ?A..?Z or byte in ?a..?z or byte in ?0..?9 or byte in [?_, ?., ?-]
    end)
  end

  defp workflow_id(candidate, policy) do
    [
      "tenant",
      candidate.tenant_ref,
      "promotion",
      candidate.candidate_id,
      "policy",
      policy.policy_id,
      "version",
      policy.version
    ]
    |> Enum.join(":")
  end

  defp idempotency_key(candidate, policy) do
    [
      "memory-promotion",
      candidate.candidate_id,
      candidate.promotion_policy_ref,
      policy.policy_id,
      "v#{policy.version}"
    ]
    |> Enum.join(":")
  end

  defp resolve_decision(candidate, %{review_required: true} = policy, workflow_plan, opts) do
    context = %{candidate: candidate, promote_policy: policy, workflow_plan: workflow_plan}

    with {:ok, review} <- call(opts, :enqueue_review, [context]),
         {:ok, review_decision} <-
           call(opts, :await_review_decision, [Map.put(context, :review, review)]) do
      build_decision(candidate, policy, review_decision, :review)
    end
  end

  defp resolve_decision(candidate, %{auto_decide: true} = policy, workflow_plan, opts) do
    context = %{candidate: candidate, promote_policy: policy, workflow_plan: workflow_plan}

    with {:ok, auto_decision} <- call(opts, :auto_decide, [context]) do
      build_decision(candidate, policy, auto_decision, :auto_decide)
    end
  end

  defp resolve_decision(_candidate, _policy, _workflow_plan, _opts),
    do: {:error, :promotion_policy_has_no_decision_path}

  defp build_decision(candidate, policy, attrs, source) when is_map(attrs) do
    attrs = normalize_attrs(attrs)

    MemoryPromotionDecision.V1.new(%{
      candidate_id: candidate.candidate_id,
      promotion_policy_ref: policy.policy_id,
      decision: Map.get(attrs, :decision),
      decision_source: source,
      source_node_ref: candidate.source_node_ref,
      commit_lsn: candidate.commit_lsn,
      commit_hlc: candidate.commit_hlc,
      review_refs: Map.get(attrs, :review_refs, []),
      evidence_refs: candidate.evidence_refs,
      governance_refs: candidate.governance_refs,
      reason: Map.get(attrs, :reason),
      metadata: Map.take(attrs, [:decision_ref, :review_ref])
    })
  end

  defp apply_decision(
         candidate,
         policy,
         workflow_plan,
         %MemoryPromotionDecision.V1{
           decision: :approved
         } = decision,
         opts
       ) do
    governed_fragment = governed_fragment(candidate, policy, decision)

    with {:ok, stored_fragment} <-
           call(opts, :insert_governed, [
             governed_fragment,
             context(candidate, policy, workflow_plan, decision)
           ]),
         {:ok, _attachment} <-
           call(opts, :emit_derived_state_attachment, [
             derived_state_attachment(stored_fragment, candidate, decision),
             context(candidate, policy, workflow_plan, decision)
           ]),
         :ok <-
           ok_callback(
             call(opts, :emit_projection_event, [
               projection_event(stored_fragment, candidate, decision),
               context(candidate, policy, workflow_plan, decision)
             ])
           ),
         {:ok, _proof} <-
           call(opts, :emit_proof, [
             proof_token(candidate, policy, workflow_plan, decision, stored_fragment),
             context(candidate, policy, workflow_plan, decision)
           ]) do
      :ok
    end
  end

  defp apply_decision(
         candidate,
         policy,
         workflow_plan,
         %MemoryPromotionDecision.V1{
           decision: :denied
         } = decision,
         opts
       ) do
    with {:ok, _proof} <-
           call(opts, :emit_proof, [
             proof_token(candidate, policy, workflow_plan, decision, nil),
             context(candidate, policy, workflow_plan, decision)
           ]) do
      :ok
    end
  end

  defp context(candidate, policy, workflow_plan, decision) do
    %{
      candidate: candidate,
      promote_policy: policy,
      workflow_plan: workflow_plan,
      decision: decision
    }
  end

  defp governed_fragment(candidate, policy, decision) do
    %{
      fragment_id: governed_fragment_id(candidate, decision),
      tenant_ref: candidate.tenant_ref,
      source_node_ref: candidate.source_node_ref,
      tier: :governed,
      t_epoch: candidate.t_epoch,
      installation_ref: candidate.installation_ref,
      source_agents: candidate.source_agents,
      source_resources: candidate.source_resources,
      source_scopes: candidate.source_scopes,
      access_agents: candidate.access_agents,
      access_resources: candidate.access_resources,
      access_scopes: candidate.access_scopes,
      access_projection_hash: candidate.access_projection_hash,
      applied_policies: candidate.applied_policies,
      evidence_refs: candidate.evidence_refs,
      governance_refs: candidate.governance_refs,
      parent_fragment_id: candidate.shared_fragment_id,
      content_hash: candidate.content_hash,
      content_ref: candidate.content_ref,
      schema_ref: candidate.schema_ref,
      promotion_decision_ref: decision.decision_id,
      promotion_policy_ref: policy.policy_id,
      rebuild_spec: candidate.rebuild_spec,
      derived_state_attachment_ref: "derived-state://memory/#{candidate.candidate_id}",
      metadata: Map.merge(candidate.metadata, %{candidate_id: candidate.candidate_id})
    }
  end

  defp governed_fragment_id(candidate, decision) do
    digest =
      "#{candidate.candidate_id}:#{decision.decision_id}"
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    "memory-governed://#{digest}"
  end

  defp derived_state_attachment(fragment, candidate, decision) do
    %{
      subject: %{
        ref: fragment.fragment_id,
        kind: "memory_governed_fragment",
        tenant_ref: candidate.tenant_ref
      },
      evidence_refs: candidate.evidence_refs,
      governance_refs: candidate.governance_refs,
      metadata: %{
        candidate_id: candidate.candidate_id,
        promotion_decision_ref: decision.decision_id,
        source_node_ref: candidate.source_node_ref,
        commit_lsn: candidate.commit_lsn,
        commit_hlc: candidate.commit_hlc
      }
    }
  end

  defp projection_event(fragment, candidate, decision) do
    %{
      event_name: "memory.promotion.projected",
      event_version: "memory-projection-event.v1",
      downstream_only?: true,
      governed_truth_owner: :mezzanine,
      tenant_ref: candidate.tenant_ref,
      installation_ref: candidate.installation_ref,
      fragment_id: fragment.fragment_id,
      candidate_id: candidate.candidate_id,
      promotion_decision_ref: decision.decision_id,
      source_node_ref: candidate.source_node_ref,
      commit_lsn: candidate.commit_lsn,
      commit_hlc: candidate.commit_hlc
    }
  end

  defp proof_token(candidate, policy, workflow_plan, decision, fragment) do
    MemoryProofToken.new!(%{
      proof_hash_version: "m7a.v1",
      proof_id: "promotion-proof://#{candidate.candidate_id}/#{decision.decision_id}",
      kind: :promote,
      tenant_ref: candidate.tenant_ref,
      installation_id: candidate.installation_ref,
      t_event: decision.decided_at,
      epoch_used: candidate.t_epoch,
      source_node_ref: candidate.source_node_ref,
      commit_lsn: candidate.commit_lsn,
      commit_hlc: candidate.commit_hlc,
      policy_refs: [%{id: policy.policy_id, version: policy.version}],
      fragment_ids: proof_fragment_ids(candidate, fragment),
      transform_hashes: [],
      access_projection_hashes: [candidate.access_projection_hash],
      trace_id: candidate.trace_id,
      parent_fragment_id: candidate.shared_fragment_id,
      child_fragment_id: fragment && fragment.fragment_id,
      evidence_refs: candidate.evidence_refs,
      governance_decision_ref: MemoryPromotionDecision.V1.dump(decision),
      metadata: %{
        promotion_status: Atom.to_string(decision.decision),
        candidate_id: candidate.candidate_id,
        review_refs: decision.review_refs,
        workflow_id: workflow_plan.workflow_id,
        workflow_task_queue: workflow_plan.task_queue,
        sidecar_projection_truth: "downstream_only"
      }
    })
  end

  defp proof_fragment_ids(candidate, nil), do: [candidate.shared_fragment_id]

  defp proof_fragment_ids(candidate, fragment),
    do: [candidate.shared_fragment_id, fragment.fragment_id]

  defp call(opts, callback, args) do
    case Keyword.get(opts, callback) do
      fun when is_function(fun, length(args)) ->
        apply(fun, args)

      nil ->
        {:error, {:missing_callback, callback}}

      _other ->
        {:error, {:invalid_callback, callback}}
    end
  end

  defp ok_callback(:ok), do: :ok
  defp ok_callback({:ok, _result}), do: :ok
  defp ok_callback({:error, reason}), do: {:error, reason}
  defp ok_callback(other), do: {:error, {:invalid_callback_result, other}}

  defp normalize_attrs(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_attrs()

  defp normalize_attrs(attrs) when is_map(attrs) do
    Map.new(attrs, fn {key, value} -> {normalize_key(key), value} end)
  end

  defp normalize_key(key) when is_atom(key), do: key

  defp normalize_key(key) when is_binary(key) do
    Map.get(@key_lookup, key, key)
  end

  defp missing_required_strings(attrs, fields) do
    Enum.reject(fields, fn field ->
      case Map.get(attrs, field) do
        value when is_binary(value) -> String.trim(value) != ""
        _value -> false
      end
    end)
  end
end
