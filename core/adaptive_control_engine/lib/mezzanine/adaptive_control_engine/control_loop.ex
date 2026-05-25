defmodule Mezzanine.AdaptiveControlEngine.ControlLoop.Receipt do
  @moduledoc "Closed-loop adaptive-control receipt."

  @enforce_keys [
    :receipt_ref,
    :fixture_refs,
    :status,
    :blocked_gate_refs,
    :control_run_ref,
    :tenant_ref,
    :source_coordination_run_ref,
    :trace_dataset_ref,
    :trace_refs,
    :replay_dataset_refs,
    :eval_dataset_refs,
    :optimization_run_ref,
    :candidate_ref,
    :optimization_target_refs,
    :role_prompt_refs,
    :verifier_prompt_refs,
    :context_budget_refs,
    :memory_policy_refs,
    :tool_policy_refs,
    :fallback_policy_refs,
    :termination_threshold_refs,
    :eval_refs,
    :replay_refs,
    :guardrail_refs,
    :budget_refs,
    :gate_evidence_refs,
    :threshold_refs,
    :shadow_ref,
    :canary_ref,
    :approval_ref,
    :promotion_ref,
    :rollback_ref,
    :stale_artifact_fence_refs,
    :artifact_lock_refs,
    :persistence_profile_ref,
    :replay_bundle_ref,
    :checkpoint_epoch_ref,
    :promotion_epoch_ref,
    :appkit_projection_refs,
    :ground_plane_fence_refs,
    :audit_refs,
    :redaction_posture
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          receipt_ref: String.t(),
          fixture_refs: [String.t()],
          status: :ready_for_promotion | :blocked,
          blocked_gate_refs: [String.t()],
          control_run_ref: String.t() | nil,
          tenant_ref: String.t() | nil,
          source_coordination_run_ref: String.t() | nil,
          trace_dataset_ref: String.t() | nil,
          trace_refs: [String.t()],
          replay_dataset_refs: [String.t()],
          eval_dataset_refs: [String.t()],
          optimization_run_ref: String.t() | nil,
          candidate_ref: String.t() | nil,
          optimization_target_refs: [String.t()],
          role_prompt_refs: [String.t()],
          verifier_prompt_refs: [String.t()],
          context_budget_refs: [String.t()],
          memory_policy_refs: [String.t()],
          tool_policy_refs: [String.t()],
          fallback_policy_refs: [String.t()],
          termination_threshold_refs: [String.t()],
          eval_refs: [String.t()],
          replay_refs: [String.t()],
          guardrail_refs: [String.t()],
          budget_refs: [String.t()],
          gate_evidence_refs: [String.t()],
          threshold_refs: [String.t()],
          shadow_ref: String.t() | nil,
          canary_ref: String.t() | nil,
          approval_ref: String.t() | nil,
          promotion_ref: String.t() | nil,
          rollback_ref: String.t() | nil,
          stale_artifact_fence_refs: [String.t()],
          artifact_lock_refs: [String.t()],
          persistence_profile_ref: String.t() | nil,
          replay_bundle_ref: String.t() | nil,
          checkpoint_epoch_ref: String.t() | nil,
          promotion_epoch_ref: String.t() | nil,
          appkit_projection_refs: [String.t()],
          ground_plane_fence_refs: [String.t()],
          audit_refs: [String.t()],
          redaction_posture: :refs_only
        }
end

defmodule Mezzanine.AdaptiveControlEngine.ControlLoop.PromotionReceipt do
  @moduledoc "Memory-promotion truth recorded by Mezzanine adaptive control."

  @enforce_keys [
    :receipt_ref,
    :status,
    :blocked_gate_refs,
    :candidate_ref,
    :promotion_ref,
    :rollback_ref,
    :tenant_ref,
    :citadel_authority_ref,
    :eval_refs,
    :trace_ref,
    :appkit_projection_ref
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          receipt_ref: String.t(),
          status: :promoted | :denied,
          blocked_gate_refs: [String.t()],
          candidate_ref: String.t() | nil,
          promotion_ref: String.t() | nil,
          rollback_ref: String.t() | nil,
          tenant_ref: String.t() | nil,
          citadel_authority_ref: String.t() | nil,
          eval_refs: [String.t()],
          trace_ref: String.t() | nil,
          appkit_projection_ref: String.t() | nil
        }
end

defmodule Mezzanine.AdaptiveControlEngine.ControlLoop.RollbackReceipt do
  @moduledoc "Memory-rollback truth recorded by Mezzanine adaptive control."

  @enforce_keys [
    :receipt_ref,
    :status,
    :blocked_gate_refs,
    :candidate_ref,
    :rollback_ref,
    :restored_ref,
    :tenant_ref,
    :citadel_authority_ref,
    :trace_ref,
    :appkit_projection_ref
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          receipt_ref: String.t(),
          status: :rolled_back | :denied,
          blocked_gate_refs: [String.t()],
          candidate_ref: String.t() | nil,
          rollback_ref: String.t() | nil,
          restored_ref: String.t() | nil,
          tenant_ref: String.t() | nil,
          citadel_authority_ref: String.t() | nil,
          trace_ref: String.t() | nil,
          appkit_projection_ref: String.t() | nil
        }
end

defmodule Mezzanine.AdaptiveControlEngine.ControlLoop do
  @moduledoc """
  Evaluates closed-loop adaptive-control promotion readiness.
  """

  alias Mezzanine.AdaptiveControlEngine.ControlLoop.{PromotionReceipt, Receipt, RollbackReceipt}

  @fixture_refs ["AOC-028", "AOC-029", "AOC-030", "PERSIST-AOC-008"]
  @required_strings [
    {:control_run_ref, "gate:control_run"},
    {:tenant_ref, "gate:tenant"},
    {:source_coordination_run_ref, "gate:trace"},
    {:trace_dataset_ref, "gate:trace_dataset"},
    {:optimization_run_ref, "gate:optimization_run"},
    {:candidate_ref, "gate:candidate"},
    {:shadow_ref, "gate:shadow"},
    {:canary_ref, "gate:canary"},
    {:promotion_ref, "gate:promotion"},
    {:rollback_ref, "gate:rollback"},
    {:persistence_profile_ref, "gate:persistence"},
    {:replay_bundle_ref, "gate:replay"},
    {:checkpoint_epoch_ref, "gate:checkpoint_epoch"},
    {:promotion_epoch_ref, "gate:promotion_epoch"}
  ]
  @required_lists [
    {:trace_refs, "gate:trace"},
    {:replay_dataset_refs, "gate:replay"},
    {:eval_dataset_refs, "gate:eval"},
    {:optimization_target_refs, "gate:optimization_target"},
    {:role_prompt_refs, "gate:role_prompt"},
    {:verifier_prompt_refs, "gate:verifier_prompt"},
    {:context_budget_refs, "gate:context_budget"},
    {:memory_policy_refs, "gate:memory_policy"},
    {:tool_policy_refs, "gate:tool_policy"},
    {:fallback_policy_refs, "gate:fallback_policy"},
    {:termination_threshold_refs, "gate:termination_threshold"},
    {:eval_refs, "gate:eval"},
    {:replay_refs, "gate:replay"},
    {:guardrail_refs, "gate:guardrail"},
    {:budget_refs, "gate:budget"},
    {:gate_evidence_refs, "gate:evidence"},
    {:threshold_refs, "gate:threshold"},
    {:operator_approval_refs, "gate:approval"},
    {:stale_artifact_fence_refs, "gate:stale_artifact_fence"},
    {:artifact_lock_refs, "gate:artifact_lock"},
    {:appkit_projection_refs, "gate:appkit_projection"},
    {:ground_plane_fence_refs, "gate:ground_plane_fence"},
    {:audit_refs, "gate:audit"}
  ]
  @consistency_gates [
    {:persistence_consistency_ref, "gate:persistence"},
    {:replay_consistency_ref, "gate:replay"},
    {:eval_consistency_ref, "gate:eval"},
    {:checkpoint_epoch_consistency_ref, "gate:checkpoint_epoch"},
    {:promotion_epoch_consistency_ref, "gate:promotion_epoch"}
  ]
  @required_gate_evidence [
    {"gate-evidence://shadow", "gate:shadow"},
    {"gate-evidence://canary", "gate:canary"},
    {"gate-evidence://eval", "gate:eval"},
    {"gate-evidence://replay", "gate:replay"},
    {"gate-evidence://guardrail", "gate:guardrail"},
    {"gate-evidence://budget", "gate:budget"},
    {"gate-evidence://approval", "gate:approval"}
  ]
  @raw_keys [
    :api_key,
    :auth_header,
    :credential_body,
    :memory_body,
    :model_output,
    :operator_private_payload,
    :provider_payload,
    :raw_model_output,
    :raw_payload,
    :raw_prompt,
    :secret,
    :token,
    "api_key",
    "auth_header",
    "credential_body",
    "memory_body",
    "model_output",
    "operator_private_payload",
    "provider_payload",
    "raw_model_output",
    "raw_payload",
    "raw_prompt",
    "secret",
    "token"
  ]

  @spec evaluate(map()) :: {:ok, Receipt.t()} | {:error, Receipt.t() | term()}
  def evaluate(attrs) when is_map(attrs) do
    with :ok <- reject_raw(attrs) do
      receipt =
        attrs
        |> blocked_gate_refs()
        |> receipt(attrs)

      case receipt.status do
        :ready_for_promotion -> {:ok, receipt}
        :blocked -> {:error, receipt}
      end
    end
  end

  def evaluate(_attrs), do: {:error, :invalid_adaptive_control_attrs}

  @spec record_promotion(map()) ::
          {:ok, PromotionReceipt.t()} | {:error, PromotionReceipt.t() | term()}
  def record_promotion(attrs) when is_map(attrs) do
    with :ok <- reject_raw(attrs) do
      blocked_gate_refs =
        attrs
        |> promotion_blocked_gate_refs()
        |> Enum.uniq()
        |> Enum.sort()

      receipt = promotion_receipt(attrs, blocked_gate_refs)

      case receipt.status do
        :promoted -> {:ok, receipt}
        :denied -> {:error, receipt}
      end
    end
  end

  def record_promotion(_attrs), do: {:error, :invalid_promotion_attrs}

  @spec record_rollback(map()) ::
          {:ok, RollbackReceipt.t()} | {:error, RollbackReceipt.t() | term()}
  def record_rollback(attrs) when is_map(attrs) do
    with :ok <- reject_raw(attrs) do
      blocked_gate_refs =
        attrs
        |> rollback_blocked_gate_refs()
        |> Enum.uniq()
        |> Enum.sort()

      receipt = rollback_receipt(attrs, blocked_gate_refs)

      case receipt.status do
        :rolled_back -> {:ok, receipt}
        :denied -> {:error, receipt}
      end
    end
  end

  def record_rollback(_attrs), do: {:error, :invalid_rollback_attrs}

  defp blocked_gate_refs(attrs) do
    string_gate_refs(attrs) ++
      list_gate_refs(attrs) ++
      consistency_gate_refs(attrs) ++
      gate_evidence_refs(attrs)
  end

  defp string_gate_refs(attrs) do
    @required_strings
    |> Enum.flat_map(fn {field, gate_ref} ->
      if present_string?(fetch(attrs, field)), do: [], else: [gate_ref]
    end)
  end

  defp list_gate_refs(attrs) do
    @required_lists
    |> Enum.flat_map(fn {field, gate_ref} ->
      if non_empty_string_list?(fetch(attrs, field)), do: [], else: [gate_ref]
    end)
  end

  defp consistency_gate_refs(attrs) do
    @consistency_gates
    |> Enum.flat_map(fn {field, gate_ref} ->
      if fetch(attrs, field) == "consistent", do: [], else: [gate_ref]
    end)
  end

  defp gate_evidence_refs(attrs) do
    gate_evidence_refs = string_list(attrs, :gate_evidence_refs)

    @required_gate_evidence
    |> Enum.flat_map(fn {evidence_ref, gate_ref} ->
      if evidence_ref in gate_evidence_refs, do: [], else: [gate_ref]
    end)
  end

  defp receipt(blocked_gate_refs, attrs) do
    blocked_gate_refs = blocked_gate_refs |> Enum.uniq() |> Enum.sort()
    control_run_ref = fetch(attrs, :control_run_ref)

    %Receipt{
      receipt_ref: "adaptive-control://" <> to_ref_segment(control_run_ref),
      fixture_refs: @fixture_refs,
      status: status(blocked_gate_refs),
      blocked_gate_refs: blocked_gate_refs,
      control_run_ref: control_run_ref,
      tenant_ref: fetch(attrs, :tenant_ref),
      source_coordination_run_ref: fetch(attrs, :source_coordination_run_ref),
      trace_dataset_ref: fetch(attrs, :trace_dataset_ref),
      trace_refs: string_list(attrs, :trace_refs),
      replay_dataset_refs: string_list(attrs, :replay_dataset_refs),
      eval_dataset_refs: string_list(attrs, :eval_dataset_refs),
      optimization_run_ref: fetch(attrs, :optimization_run_ref),
      candidate_ref: fetch(attrs, :candidate_ref),
      optimization_target_refs: string_list(attrs, :optimization_target_refs),
      role_prompt_refs: string_list(attrs, :role_prompt_refs),
      verifier_prompt_refs: string_list(attrs, :verifier_prompt_refs),
      context_budget_refs: string_list(attrs, :context_budget_refs),
      memory_policy_refs: string_list(attrs, :memory_policy_refs),
      tool_policy_refs: string_list(attrs, :tool_policy_refs),
      fallback_policy_refs: string_list(attrs, :fallback_policy_refs),
      termination_threshold_refs: string_list(attrs, :termination_threshold_refs),
      eval_refs: string_list(attrs, :eval_refs),
      replay_refs: string_list(attrs, :replay_refs),
      guardrail_refs: string_list(attrs, :guardrail_refs),
      budget_refs: string_list(attrs, :budget_refs),
      gate_evidence_refs: string_list(attrs, :gate_evidence_refs),
      threshold_refs: string_list(attrs, :threshold_refs),
      shadow_ref: fetch(attrs, :shadow_ref),
      canary_ref: fetch(attrs, :canary_ref),
      approval_ref: first_string(attrs, :operator_approval_refs),
      promotion_ref: fetch(attrs, :promotion_ref),
      rollback_ref: fetch(attrs, :rollback_ref),
      stale_artifact_fence_refs: string_list(attrs, :stale_artifact_fence_refs),
      artifact_lock_refs: string_list(attrs, :artifact_lock_refs),
      persistence_profile_ref: fetch(attrs, :persistence_profile_ref),
      replay_bundle_ref: fetch(attrs, :replay_bundle_ref),
      checkpoint_epoch_ref: fetch(attrs, :checkpoint_epoch_ref),
      promotion_epoch_ref: fetch(attrs, :promotion_epoch_ref),
      appkit_projection_refs: string_list(attrs, :appkit_projection_refs),
      ground_plane_fence_refs: string_list(attrs, :ground_plane_fence_refs),
      audit_refs: string_list(attrs, :audit_refs),
      redaction_posture: :refs_only
    }
  end

  defp status([]), do: :ready_for_promotion
  defp status([_ | _]), do: :blocked

  defp promotion_blocked_gate_refs(attrs) do
    required_string_gate_refs(attrs, [
      {:candidate_ref, "gate:candidate"},
      {:promotion_ref, "gate:promotion"},
      {:rollback_ref, "gate:rollback"},
      {:tenant_ref, "gate:tenant"},
      {:citadel_authority_ref, "gate:citadel_authority"},
      {:trace_ref, "gate:trace"},
      {:appkit_projection_ref, "gate:appkit_projection"}
    ]) ++ required_list_gate_refs(attrs, [{:eval_refs, "gate:eval"}])
  end

  defp rollback_blocked_gate_refs(attrs) do
    required_string_gate_refs(attrs, [
      {:candidate_ref, "gate:candidate"},
      {:rollback_ref, "gate:rollback"},
      {:restored_ref, "gate:restored_memory"},
      {:tenant_ref, "gate:tenant"},
      {:citadel_authority_ref, "gate:citadel_authority"},
      {:trace_ref, "gate:trace"},
      {:appkit_projection_ref, "gate:appkit_projection"}
    ])
  end

  defp required_string_gate_refs(attrs, fields) do
    Enum.flat_map(fields, fn {field, gate_ref} ->
      if present_string?(fetch(attrs, field)), do: [], else: [gate_ref]
    end)
  end

  defp required_list_gate_refs(attrs, fields) do
    Enum.flat_map(fields, fn {field, gate_ref} ->
      if string_list(attrs, field) != [], do: [], else: [gate_ref]
    end)
  end

  defp promotion_receipt(attrs, blocked_gate_refs) do
    %PromotionReceipt{
      receipt_ref:
        "adaptive-control-promotion://" <> to_ref_segment(fetch(attrs, :promotion_ref)),
      status: promotion_status(blocked_gate_refs),
      blocked_gate_refs: blocked_gate_refs,
      candidate_ref: fetch(attrs, :candidate_ref),
      promotion_ref: fetch(attrs, :promotion_ref),
      rollback_ref: fetch(attrs, :rollback_ref),
      tenant_ref: fetch(attrs, :tenant_ref),
      citadel_authority_ref: fetch(attrs, :citadel_authority_ref),
      eval_refs: string_list(attrs, :eval_refs),
      trace_ref: fetch(attrs, :trace_ref),
      appkit_projection_ref: fetch(attrs, :appkit_projection_ref)
    }
  end

  defp rollback_receipt(attrs, blocked_gate_refs) do
    %RollbackReceipt{
      receipt_ref: "adaptive-control-rollback://" <> to_ref_segment(fetch(attrs, :rollback_ref)),
      status: rollback_status(blocked_gate_refs),
      blocked_gate_refs: blocked_gate_refs,
      candidate_ref: fetch(attrs, :candidate_ref),
      rollback_ref: fetch(attrs, :rollback_ref),
      restored_ref: fetch(attrs, :restored_ref),
      tenant_ref: fetch(attrs, :tenant_ref),
      citadel_authority_ref: fetch(attrs, :citadel_authority_ref),
      trace_ref: fetch(attrs, :trace_ref),
      appkit_projection_ref: fetch(attrs, :appkit_projection_ref)
    }
  end

  defp promotion_status([]), do: :promoted
  defp promotion_status([_ | _]), do: :denied

  defp rollback_status([]), do: :rolled_back
  defp rollback_status([_ | _]), do: :denied

  defp reject_raw(attrs) do
    case raw_key(attrs) do
      nil -> :ok
      key -> {:error, {:forbidden_raw_field, key}}
    end
  end

  defp raw_key(%_struct{} = value), do: value |> Map.from_struct() |> raw_key()

  defp raw_key(value) when is_map(value) do
    Enum.find_value(value, fn {key, nested} ->
      if key in @raw_keys, do: key, else: raw_key(nested)
    end)
  end

  defp raw_key(value) when is_list(value), do: Enum.find_value(value, &raw_key/1)
  defp raw_key(_value), do: nil

  defp first_string(attrs, field) do
    attrs
    |> string_list(field)
    |> List.first()
  end

  defp string_list(attrs, field) do
    case fetch(attrs, field, []) do
      values when is_list(values) and values != [] ->
        if Enum.all?(values, &present_string?/1), do: values, else: []

      _other ->
        []
    end
  end

  defp non_empty_string_list?(values) when is_list(values) and values != [] do
    Enum.all?(values, &present_string?/1)
  end

  defp non_empty_string_list?(_values), do: false
  defp present_string?(value), do: is_binary(value) and String.trim(value) != ""

  defp fetch(attrs, field, default \\ nil) do
    string_field = Atom.to_string(field)

    cond do
      Map.has_key?(attrs, field) -> Map.fetch!(attrs, field)
      Map.has_key?(attrs, string_field) -> Map.fetch!(attrs, string_field)
      true -> default
    end
  end

  defp to_ref_segment(value) when is_binary(value) and value != "", do: value
  defp to_ref_segment(_value), do: "missing"
end
