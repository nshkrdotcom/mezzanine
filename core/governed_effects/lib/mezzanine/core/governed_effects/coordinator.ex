defmodule Mezzanine.Core.GovernedEffects.Coordinator do
  @moduledoc """
  Pure coordinator for the governed-effect lifecycle.

  The coordinator owns sequencing and state transitions. Citadel and lower
  execution systems remain boundary adapters supplied by callers until later
  phases wire production owners.
  """

  alias Mezzanine.Core.GovernedEffects.AuthorityPacket
  alias Mezzanine.Core.GovernedEffects.Coordinator.Run
  alias Mezzanine.Core.GovernedEffects.EffectLog
  alias Mezzanine.Core.GovernedEffects.EffectReceipt
  alias Mezzanine.Core.GovernedEffects.GovernedEffect
  alias Mezzanine.Core.GovernedEffects.Projection
  alias Mezzanine.Core.GovernedEffects.Support
  alias Mezzanine.Core.GovernedEffects.TransitionGate

  @terminal_statuses [:completed, :denied]
  @citadel_decisions %{
    "allow" => :allow,
    "deny" => :deny,
    "review" => :review,
    "review_required" => :review,
    "downgrade" => :downgrade,
    "revoke" => :revoke
  }

  @spec propose(map() | keyword(), keyword()) ::
          {:ok, Run.t()} | {:legacy_bypass, map()} | {:error, term()}
  def propose(command, opts \\ []) do
    if Keyword.get(opts, :governed_effects_enabled, true) do
      create_proposal(command)
    else
      {:legacy_bypass, %{reason: :governed_effects_not_enabled, command: command}}
    end
  end

  @spec authorize(Run.t(), map() | keyword()) :: {:ok, Run.t()} | {:error, term()}
  def authorize(%Run{} = run, decision_or_opts) do
    with {:ok, decision} <- resolve_authority(run, decision_or_opts),
         {:ok, packet, metadata} <- map_authority_decision(decision),
         :ok <- require_decision(packet, :allow),
         {:ok, effect} <- transition(run.effect, :authorized, authority_ref: packet.authority_ref),
         {:ok, run} <-
           record_transition(%{
             run
             | effect: effect,
               authority_packet: packet,
               authority_metadata: metadata
           }) do
      {:ok, refresh_projection(run)}
    end
  end

  @spec deny(Run.t(), map() | keyword()) :: {:ok, Run.t()} | {:error, term()}
  def deny(%Run{} = run, decision) do
    with {:ok, packet, metadata} <- map_authority_decision(decision),
         :ok <- require_decision(packet, :deny),
         {:ok, effect} <- transition(run.effect, :denied, authority_ref: packet.authority_ref),
         {:ok, run} <-
           record_transition(%{
             run
             | effect: effect,
               authority_packet: packet,
               authority_metadata: metadata
           }) do
      {:ok, refresh_projection(run)}
    end
  end

  @spec dispatch(Run.t(), keyword()) :: {:ok, Run.t()} | {:error, term()}
  def dispatch(run, opts \\ [])

  def dispatch(%Run{effect: %{status: status}}, _opts) when status in @terminal_statuses,
    do: {:error, {:terminal_effect, status}}

  def dispatch(%Run{} = run, opts) do
    with {:ok, effect} <- transition(run.effect, :dispatched),
         envelope <- invocation_envelope(%{run | effect: effect}),
         {:ok, envelope} <- dispatch_envelope(envelope, opts),
         {:ok, run} <- record_transition(%{run | effect: effect, invocation_envelope: envelope}) do
      {:ok, refresh_projection(run)}
    end
  end

  @spec receive_receipt(Run.t(), map() | keyword()) :: {:ok, Run.t()} | {:error, term()}
  def receive_receipt(%Run{} = run, receipt_attrs) do
    with {:ok, receipt} <- EffectReceipt.new(receipt_attrs),
         {:ok, effect} <-
           transition(run.effect, :receipt_received,
             receipt_ref: receipt.receipt_ref,
             dispatch_ref: dispatch_ref(run.invocation_envelope)
           ),
         {:ok, run} <- record_transition(%{run | effect: effect, receipt: receipt}),
         {:ok, run} <- record_receipt(run) do
      {:ok, refresh_projection(run)}
    end
  end

  @spec reduce(Run.t()) :: {:ok, Run.t()} | {:error, term()}
  def reduce(%Run{} = run) do
    with {:ok, effect} <- transition(run.effect, :reduced),
         reduced_facts <- reduced_facts(run),
         {:ok, run} <- record_transition(%{run | effect: effect, reduced_facts: reduced_facts}) do
      {:ok, refresh_projection(run)}
    end
  end

  @spec project(Run.t()) :: {:ok, Run.t()} | {:error, term()}
  def project(%Run{} = run) do
    with {:ok, effect} <- transition(run.effect, :projected),
         {:ok, run} <- record_transition(%{run | effect: effect}),
         {:ok, run} <- record_projection(run) do
      {:ok, refresh_projection(run)}
    end
  end

  @spec complete(Run.t()) :: {:ok, Run.t()} | {:error, term()}
  def complete(%Run{} = run) do
    with {:ok, effect} <- transition(run.effect, :completed),
         {:ok, run} <- record_transition(%{run | effect: effect}) do
      {:ok, refresh_projection(run)}
    end
  end

  @spec map_authority_decision(map() | keyword()) ::
          {:ok, AuthorityPacket.t(), map()} | {:error, term()}
  def map_authority_decision(decision) do
    with {:ok, attrs} <- Support.normalize_attrs(decision),
         {:ok, internal_decision} <- decision_value(Support.required(attrs, :decision)),
         {:ok, packet} <-
           AuthorityPacket.new(
             authority_ref: Support.required(attrs, :authority_ref),
             decision: internal_decision,
             tenant_ref: Support.required(attrs, :tenant_ref),
             actor_ref: Support.required(attrs, :actor_ref),
             command_ref: Support.optional(attrs, :command_ref),
             trace_ref: Support.optional(attrs, :trace_ref),
             policy_refs: Support.optional(attrs, :policy_refs, []),
             risk_class: Support.optional(attrs, :risk_class),
             budget_refs: Support.optional(attrs, :budget_refs, []),
             residency_refs: Support.optional(attrs, :residency_refs, []),
             reason: Support.optional(attrs, :reason),
             expiry: Support.optional(attrs, :expiry)
           ) do
      {:ok, packet, authority_metadata(attrs)}
    end
  end

  defp create_proposal(command) do
    with {:ok, attrs} <- Support.normalize_attrs(command),
         {:ok, effect} <- GovernedEffect.new(effect_attrs(attrs)),
         {:ok, log} <- EffectLog.new(trace_ref: effect.trace_ref),
         {:ok, run} <- record_transition(%Run{command: attrs, effect: effect, log: log}) do
      {:ok, refresh_projection(run)}
    end
  end

  defp effect_attrs(attrs) do
    [
      effect_ref: Support.required(attrs, :effect_ref),
      effect_type: Support.required(attrs, :effect_type),
      command_ref: Support.required(attrs, :command_ref),
      tenant_ref: Support.required(attrs, :tenant_ref),
      actor_ref: Support.optional(attrs, :actor_ref),
      installation_ref: Support.optional(attrs, :installation_ref),
      risk_class: Support.optional(attrs, :risk_class),
      preconditions: Support.optional(attrs, :preconditions, []),
      expected_version: Support.optional(attrs, :expected_version, 1),
      trace_ref: Support.required(attrs, :trace_ref),
      status: :proposed
    ]
  end

  defp resolve_authority(%Run{} = run, opts) when is_list(opts) do
    case Keyword.fetch(opts, :authority_adapter) do
      {:ok, adapter} when is_function(adapter, 1) -> normalize_adapter_result(adapter.(run))
      :error -> {:ok, opts}
    end
  end

  defp resolve_authority(_run, decision), do: {:ok, decision}

  defp normalize_adapter_result({:ok, decision}), do: {:ok, decision}
  defp normalize_adapter_result({:error, reason}), do: {:error, reason}
  defp normalize_adapter_result(decision), do: {:ok, decision}

  defp require_decision(%AuthorityPacket{decision: expected}, expected), do: :ok

  defp require_decision(%AuthorityPacket{decision: actual}, expected),
    do: {:error, {:unexpected_authority_decision, %{expected: expected, actual: actual}}}

  defp transition(effect, next_status, opts \\ []) do
    transition_opts =
      opts
      |> Keyword.put_new(:expected_version, effect.expected_version)
      |> maybe_put(:authority_ref, Keyword.get(opts, :authority_ref))

    with {:ok, effect} <- TransitionGate.transition(effect, next_status, transition_opts) do
      effect
      |> Map.from_struct()
      |> Map.merge(Map.take(Map.new(opts), [:dispatch_ref, :receipt_ref]))
      |> GovernedEffect.new()
    end
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp record_transition(%Run{} = run) do
    append_log(run, %{
      event_kind: :effect_transition,
      status: run.effect.status,
      payload: %{
        "authority_ref" => run.effect.authority_ref,
        "dispatch_ref" => run.effect.dispatch_ref,
        "effect_type" => run.effect.effect_type,
        "receipt_ref" => run.effect.receipt_ref,
        "status" => run.effect.status
      }
    })
  end

  defp record_receipt(%Run{} = run) do
    append_log(run, %{
      event_kind: :receipt_reduced,
      status: run.receipt.status,
      payload: %{
        "evidence_refs" => run.receipt.evidence_refs,
        "receipt_ref" => run.receipt.receipt_ref,
        "status" => run.receipt.status
      }
    })
  end

  defp record_projection(%Run{} = run) do
    append_log(run, %{
      event_kind: :projection_updated,
      status: run.effect.status,
      payload: %{
        "projection_keys" => projection_keys(Projection.product_safe(run)),
        "status" => run.effect.status
      }
    })
  end

  defp append_log(%Run{log: log, effect: effect} = run, attrs) do
    attrs =
      Map.merge(attrs, %{
        effect_ref: effect.effect_ref,
        tenant_ref: effect.tenant_ref,
        trace_ref: effect.trace_ref
      })

    with {:ok, log, _entry} <- EffectLog.append(log, attrs) do
      {:ok, %{run | log: log}}
    end
  end

  defp invocation_envelope(%Run{} = run) do
    %{
      "authority_ref" => run.effect.authority_ref,
      "boundary_class" => Map.get(run.authority_metadata, "boundary_class"),
      "effect_ref" => run.effect.effect_ref,
      "effect_type" => run.effect.effect_type,
      "invocation_ref" => invocation_ref(run.effect),
      "operation" => command_value(run.command, :operation),
      "payload" => command_value(run.command, :payload) || %{},
      "posture" => Map.get(run.authority_metadata, "posture"),
      "tenant_ref" => run.effect.tenant_ref,
      "trace_ref" => run.effect.trace_ref
    }
    |> drop_nil_values()
  end

  defp dispatch_envelope(envelope, opts) do
    case Keyword.fetch(opts, :dispatch_adapter) do
      {:ok, adapter} when is_function(adapter, 1) -> normalize_adapter_result(adapter.(envelope))
      :error -> {:ok, envelope}
    end
  end

  defp dispatch_ref(nil), do: nil

  defp dispatch_ref(envelope),
    do: Map.get(envelope, "dispatch_ref", Map.get(envelope, "invocation_ref"))

  defp reduced_facts(%Run{} = run) do
    %{
      "effect_ref" => run.effect.effect_ref,
      "receipt_ref" => run.receipt.receipt_ref,
      "receipt_status" => Atom.to_string(run.receipt.status)
    }
  end

  defp refresh_projection(%Run{} = run), do: %{run | projection: Projection.product_safe(run)}

  defp decision_value(value) when is_atom(value), do: decision_value(Atom.to_string(value))

  defp decision_value(value) when is_binary(value) do
    case Map.fetch(@citadel_decisions, value) do
      {:ok, decision} -> {:ok, decision}
      :error -> {:error, {:invalid_authority_decision, value}}
    end
  end

  defp decision_value(value), do: {:error, {:invalid_authority_decision, value}}

  defp authority_metadata(attrs) do
    %{}
    |> metadata_put("decision_hash", Support.optional(attrs, :decision_hash))
    |> metadata_put("boundary_class", Support.optional(attrs, :boundary_class))
    |> metadata_put("posture", Support.optional(attrs, :posture))
  end

  defp metadata_put(metadata, _key, nil), do: metadata
  defp metadata_put(metadata, key, value), do: Map.put(metadata, key, value)

  defp invocation_ref(%GovernedEffect{} = effect) do
    effect.effect_ref
    |> String.replace("effect://", "invocation://")
  end

  defp command_value(command, key),
    do: Map.get(command, key, Map.get(command, Atom.to_string(key)))

  defp projection_keys(projection) do
    projection |> Map.keys() |> Enum.sort()
  end

  defp drop_nil_values(map), do: Map.reject(map, fn {_key, value} -> is_nil(value) end)
end
