defmodule Mezzanine.OperatorCommands do
  @moduledoc """
  Durable operator pause, resume, and cancel commands on the substrate path.
  """

  require Ash.Query

  alias Mezzanine.Execution.{DispatchState, ExecutionRecord, OperatorActionClassification, Repo}
  alias Mezzanine.Leasing
  alias Mezzanine.Objects.SubjectRecord

  @type result :: {:ok, map()} | {:error, term()}

  @spec pause(Ecto.UUID.t(), keyword()) :: result()
  def pause(subject_id, opts \\ []) when is_binary(subject_id) and is_list(opts) do
    now = now(opts)

    with {:ok, subject} <- fetch_subject(subject_id) do
      pause_subject(subject_id, subject, opts, now)
    end
  end

  @spec resume(Ecto.UUID.t(), keyword()) :: result()
  def resume(subject_id, opts \\ []) when is_binary(subject_id) and is_list(opts) do
    with {:ok, subject} <- fetch_subject(subject_id) do
      resume_subject(subject_id, subject, opts)
    end
  end

  @spec cancel(Ecto.UUID.t(), keyword()) :: result()
  def cancel(subject_id, opts \\ []) when is_binary(subject_id) and is_list(opts) do
    now = now(opts)
    cancel_reason = keyword_reason(opts)
    trace_id = trace_id(opts, subject_id, "cancel")
    causation_id = causation_id(opts, "cancel", subject_id)
    actor_ref = actor_ref(opts)

    with {:ok, subject} <- fetch_subject(subject_id) do
      cancel_subject(subject_id, subject, cancel_reason, trace_id, causation_id, actor_ref, now)
    end
  end

  defp pause_subject(subject_id, subject, opts, now) do
    case subject.status do
      "paused" ->
        build_result(:pause, subject, workflow_signal_refs: [], noop?: true)

      "cancelled" ->
        {:error, {:subject_terminal, subject_id, "cancelled"}}

      _active ->
        pause_active_subject(subject, opts, now)
    end
  end

  defp pause_active_subject(subject, opts, now) do
    subject_id = subject.id
    trace_id = trace_id(opts, subject_id, "pause")
    causation_id = causation_id(opts, "pause", subject_id)
    action_context = action_context(:pause, subject, opts, trace_id, causation_id)

    with {:ok, active_executions} <- fetch_active_executions(subject_id),
         {:ok, workflow_signal_actions} <-
           workflow_signal_actions(:pause, active_executions, action_context),
         workflow_signal_refs = action_refs(workflow_signal_actions),
         {:ok, invalidations} <-
           invalidate_subject_leases(subject_id, "subject_paused", trace_id, now),
         {:ok, local_mutations} <-
           local_mutations([
             {:subject_status, :pause, "subject://#{subject_id}", action_context},
             lease_invalidation_mutation(subject_id, invalidations, action_context)
           ]),
         operator_actions = workflow_signal_actions ++ local_mutations,
         {:ok, updated_subject} <-
           SubjectRecord.pause(
             subject,
             %{
               reason: keyword_reason(opts),
               trace_id: trace_id,
               causation_id: causation_id,
               actor_ref: actor_ref(opts),
               operator_context: %{
                 workflow_signal_refs: workflow_signal_refs,
                 workflow_signal_actions: workflow_signal_actions,
                 local_mutations: local_mutations,
                 operator_actions: operator_actions,
                 invalidated_lease_ids: Enum.map(invalidations, & &1.lease_id)
               }
             }
           ) do
      build_result(:pause, updated_subject,
        workflow_signal_refs: workflow_signal_refs,
        workflow_signal_actions: workflow_signal_actions,
        local_mutations: local_mutations,
        operator_actions: operator_actions,
        invalidated_lease_ids: Enum.map(invalidations, & &1.lease_id),
        noop?: false
      )
    end
  end

  defp resume_subject(subject_id, subject, opts) do
    case subject.status do
      "active" ->
        build_result(:resume, subject, workflow_signal_refs: [], noop?: true)

      "cancelled" ->
        {:error, {:subject_terminal, subject_id, "cancelled"}}

      _paused ->
        resume_paused_subject(subject_id, subject, opts)
    end
  end

  defp resume_paused_subject(subject_id, subject, opts) do
    trace_id = trace_id(opts, subject_id, "resume")
    causation_id = causation_id(opts, "resume", subject_id)
    action_context = action_context(:resume, subject, opts, trace_id, causation_id)

    with {:ok, active_executions} <- fetch_active_executions(subject_id),
         {:ok, workflow_signal_actions} <-
           workflow_signal_actions(:resume, active_executions, action_context),
         workflow_signal_refs = action_refs(workflow_signal_actions),
         {:ok, local_mutations} <-
           local_mutations([
             {:subject_status, :resume, "subject://#{subject_id}", action_context}
           ]),
         operator_actions = workflow_signal_actions ++ local_mutations,
         {:ok, updated_subject} <-
           SubjectRecord.resume(
             subject,
             %{
               trace_id: trace_id,
               causation_id: causation_id,
               actor_ref: actor_ref(opts),
               operator_context: %{
                 workflow_signal_refs: workflow_signal_refs,
                 workflow_signal_actions: workflow_signal_actions,
                 local_mutations: local_mutations,
                 operator_actions: operator_actions
               }
             }
           ) do
      build_result(:resume, updated_subject,
        workflow_signal_refs: workflow_signal_refs,
        workflow_signal_actions: workflow_signal_actions,
        local_mutations: local_mutations,
        operator_actions: operator_actions,
        noop?: false
      )
    end
  end

  defp cancel_subject(
         subject_id,
         subject,
         cancel_reason,
         trace_id,
         causation_id,
         actor_ref,
         now
       ) do
    case subject.status do
      "cancelled" ->
        build_result(:cancel, subject,
          cancelled_execution_ids: [],
          workflow_signal_refs: [],
          noop?: true
        )

      _active ->
        cancel_active_subject(
          subject_id,
          subject,
          cancel_reason,
          trace_id,
          causation_id,
          actor_ref,
          now
        )
    end
  end

  defp cancel_active_subject(
         subject_id,
         subject,
         cancel_reason,
         trace_id,
         causation_id,
         actor_ref,
         now
       ) do
    action_context = %{
      action: :cancel,
      subject_id: subject_id,
      installation_id: subject.installation_id,
      trace_id: trace_id,
      causation_id: causation_id,
      actor_ref: actor_ref,
      reason: cancel_reason
    }

    with {:ok, active_executions} <- fetch_active_executions(subject_id),
         {workflow_executions, local_executions} <-
           split_workflow_and_local_executions(active_executions),
         {:ok, workflow_signal_actions} <-
           workflow_signal_actions(:cancel, workflow_executions, action_context),
         workflow_signal_refs = action_refs(workflow_signal_actions),
         {:ok, cancelled_execution_ids, execution_mutations} <-
           cancel_active_executions(
             local_executions,
             cancel_reason,
             trace_id,
             causation_id,
             actor_ref
           ),
         {:ok, invalidations} <-
           invalidate_subject_leases(subject_id, "subject_cancelled", trace_id, now),
         {:ok, declared_local_mutations} <-
           local_mutations([
             {:subject_status, :cancel, "subject://#{subject_id}", action_context},
             lease_invalidation_mutation(subject_id, invalidations, action_context)
           ]),
         local_mutations = execution_mutations ++ declared_local_mutations,
         operator_actions = workflow_signal_actions ++ local_mutations,
         {:ok, updated_subject} <-
           SubjectRecord.cancel(
             subject,
             %{
               reason: cancel_reason,
               trace_id: trace_id,
               causation_id: causation_id,
               actor_ref: actor_ref,
               operator_context: %{
                 cancelled_execution_ids: cancelled_execution_ids,
                 workflow_signal_refs: workflow_signal_refs,
                 workflow_signal_actions: workflow_signal_actions,
                 local_mutations: local_mutations,
                 operator_actions: operator_actions,
                 invalidated_lease_ids: Enum.map(invalidations, & &1.lease_id)
               }
             }
           ) do
      build_result(
        :cancel,
        updated_subject,
        cancelled_execution_ids: cancelled_execution_ids,
        workflow_signal_refs: workflow_signal_refs,
        workflow_signal_actions: workflow_signal_actions,
        local_mutations: local_mutations,
        operator_actions: operator_actions,
        invalidated_lease_ids: Enum.map(invalidations, & &1.lease_id),
        noop?: false
      )
    end
  end

  defp invalidate_subject_leases(subject_id, reason, trace_id, now) do
    case Leasing.invalidate_subject_leases(
           subject_id,
           reason,
           now: now,
           repo: Repo,
           trace_id: trace_id
         ) do
      {:ok, invalidations} -> {:ok, invalidations}
      {:error, error} -> {:error, error}
    end
  end

  defp fetch_subject(subject_id) do
    SubjectRecord
    |> Ash.Query.filter(id == ^subject_id)
    |> Ash.read(authorize?: false, domain: Mezzanine.Objects)
    |> case do
      {:ok, [subject]} -> {:ok, subject}
      {:ok, []} -> {:error, {:subject_not_found, subject_id}}
      {:error, error} -> {:error, error}
    end
  end

  defp fetch_active_executions(subject_id) do
    case ExecutionRecord.active_for_subject(subject_id) do
      {:ok, executions} -> {:ok, executions}
      {:error, error} -> {:error, error}
    end
  end

  defp cancel_active_executions(executions, reason, trace_id, causation_id, actor_ref) do
    executions
    |> Enum.reduce_while({:ok, [], []}, fn execution, {:ok, cancelled_ids, local_mutations} ->
      case record_operator_cancelled_mutation(
             execution,
             reason,
             trace_id,
             causation_id,
             actor_ref
           ) do
        {:ok, cancelled_execution, mutation} ->
          {:cont, {:ok, [cancelled_execution.id | cancelled_ids], [mutation | local_mutations]}}

        {:error, error} ->
          {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, cancelled_ids, local_mutations} ->
        {:ok, Enum.reverse(cancelled_ids), Enum.reverse(local_mutations)}

      {:error, error} ->
        {:error, error}
    end
  end

  defp record_operator_cancelled_mutation(execution, reason, trace_id, causation_id, actor_ref) do
    cancel_context = %{
      reason: reason,
      trace_id: trace_id,
      causation_id: causation_id,
      actor_ref: actor_ref
    }

    with {:ok, cancelled_execution} <-
           ExecutionRecord.record_operator_cancelled(execution, cancel_context),
         {:ok, mutation} <-
           execution_cancel_mutation(
             cancelled_execution,
             reason,
             trace_id,
             causation_id,
             actor_ref
           ) do
      {:ok, cancelled_execution, mutation}
    end
  end

  defp workflow_signal_actions(action, executions, context) do
    executions
    |> Enum.filter(&workflow_managed_execution?/1)
    |> Enum.reduce_while({:ok, []}, fn execution, {:ok, actions} ->
      case OperatorActionClassification.workflow_signal(action, execution, context) do
        {:ok, signal_action} -> {:cont, {:ok, [signal_action | actions]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, actions} -> {:ok, Enum.reverse(actions)}
      {:error, error} -> {:error, error}
    end
  end

  defp split_workflow_and_local_executions(executions) do
    Enum.split_with(executions, &workflow_managed_execution?/1)
  end

  defp workflow_managed_execution?(execution) do
    DispatchState.canonical(execution.dispatch_state) == :accepted_active and
      execution.submission_ref != %{}
  end

  defp action_refs(actions), do: Enum.map(actions, &OperatorActionClassification.action_ref/1)

  defp local_mutations(mutations) do
    mutations
    |> Enum.reject(&is_nil/1)
    |> Enum.reduce_while({:ok, []}, fn {owner_key, owner_action, target_ref, context},
                                       {:ok, acc} ->
      case OperatorActionClassification.declared_local_mutation(
             owner_key,
             owner_action,
             target_ref,
             context
           ) do
        {:ok, mutation} -> {:cont, {:ok, [mutation | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, mutations} -> {:ok, Enum.reverse(mutations)}
      {:error, error} -> {:error, error}
    end
  end

  defp lease_invalidation_mutation(_subject_id, [], _context), do: nil

  defp lease_invalidation_mutation(subject_id, invalidations, context) do
    context =
      Map.put(context, :metadata, %{
        invalidated_lease_ids: Enum.map(invalidations, & &1.lease_id)
      })

    {:lease_invalidation, :invalidate_subject_leases, "subject-leases://#{subject_id}", context}
  end

  defp execution_cancel_mutation(execution, reason, trace_id, causation_id, actor_ref) do
    OperatorActionClassification.declared_local_mutation(
      :execution_cancel,
      :record_operator_cancelled,
      "execution://#{execution.id}",
      %{
        action: :cancel,
        subject_id: execution.subject_id,
        trace_id: trace_id,
        causation_id: causation_id,
        actor_ref: actor_ref,
        reason: reason
      }
    )
  end

  defp action_context(action, subject, opts, trace_id, causation_id) do
    %{
      action: action,
      subject_id: subject.id,
      installation_id: subject.installation_id,
      trace_id: trace_id,
      causation_id: causation_id,
      actor_ref: actor_ref(opts),
      reason: keyword_reason(opts)
    }
  end

  defp build_result(action, subject, details) do
    details =
      details
      |> Map.new()
      |> Map.put_new(:workflow_signal_actions, [])
      |> Map.put_new(:local_mutations, [])
      |> Map.put_new(:operator_actions, [])

    {:ok,
     %{
       action: action,
       subject_id: subject.id,
       installation_id: subject.installation_id,
       lifecycle_state: subject.lifecycle_state,
       status: subject.status,
       status_reason: subject.status_reason,
       status_updated_at: subject.status_updated_at,
       terminal_at: subject.terminal_at,
       details: details
     }}
  end

  defp now(opts) do
    opts
    |> Keyword.get(:now, DateTime.utc_now())
    |> DateTime.truncate(:microsecond)
  end

  defp trace_id(opts, subject_id, action) do
    Keyword.get(opts, :trace_id, "operator-commands:#{action}:#{subject_id}")
  end

  defp causation_id(opts, action, subject_id) do
    Keyword.get(opts, :causation_id, "operator-commands:#{action}:#{subject_id}")
  end

  defp actor_ref(opts) do
    opts
    |> Keyword.get(:actor_ref, %{kind: :operator})
    |> normalize_map()
  end

  defp keyword_reason(opts), do: Keyword.get(opts, :reason)

  defp normalize_map(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_map(_other), do: %{}

  defp normalize_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp normalize_value(value) when is_binary(value), do: value
  defp normalize_value(value) when is_map(value), do: normalize_map(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value
end
