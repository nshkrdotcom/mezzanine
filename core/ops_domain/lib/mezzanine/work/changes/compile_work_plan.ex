defmodule Mezzanine.Work.Changes.CompileWorkPlan do
  @moduledoc false

  use Ash.Resource.Change

  alias Mezzanine.Planner
  alias Mezzanine.Programs.PolicyBundle, as: PolicyBundleResource
  alias Mezzanine.Work.{WorkClass, WorkObject, WorkPlan}
  alias MezzanineOpsModel.Codec
  alias MezzanineOpsModel.PolicyBundle, as: ModelPolicyBundle
  alias MezzanineOpsModel.WorkObject, as: ModelWorkObject

  @impl true
  def change(changeset, _opts, context) do
    Ash.Changeset.before_action(changeset, fn changeset ->
      tenant = changeset.tenant
      actor = context.actor

      with {:ok, work_object} <- fetch_work_object(changeset, tenant, actor),
           {:ok, work_class} <- fetch_work_class(work_object, tenant, actor),
           {:ok, policy_bundle} <- fetch_policy_bundle(changeset, work_class, tenant, actor),
           {:ok, plan} <-
             Planner.compile(
               to_model_work_object(work_object, work_class),
               to_model_policy_bundle(policy_bundle)
             ),
           {:ok, version} <- next_version(work_object.id, tenant, actor) do
        changeset
        |> Ash.Changeset.force_change_attribute(:tenant_id, work_object.tenant_id)
        |> Ash.Changeset.force_change_attribute(:policy_bundle_id, policy_bundle.id)
        |> Ash.Changeset.force_change_attribute(:version, version)
        |> Ash.Changeset.force_change_attribute(:status, :compiled)
        |> Ash.Changeset.force_change_attribute(:plan_payload, Codec.dump(plan))
        |> Ash.Changeset.force_change_attribute(
          :derived_run_intents,
          Codec.dump(plan.derived_run_intents)
        )
        |> Ash.Changeset.force_change_attribute(
          :derived_review_intents,
          Codec.dump(plan.derived_review_intents)
        )
        |> Ash.Changeset.force_change_attribute(
          :derived_effect_intents,
          Codec.dump(plan.derived_effect_intents)
        )
        |> Ash.Changeset.force_change_attribute(
          :derived_read_intents,
          Codec.dump(plan.derived_read_intents)
        )
        |> Ash.Changeset.force_change_attribute(
          :derived_notification_intents,
          Codec.dump(plan.derived_notification_intents)
        )
        |> Ash.Changeset.force_change_attribute(
          :obligation_ids,
          Enum.map(plan.obligations, & &1.obligation_id)
        )
        |> Ash.Changeset.force_change_attribute(
          :compiled_at,
          DateTime.utc_now() |> DateTime.truncate(:second)
        )
        |> Ash.Changeset.force_change_attribute(:metadata, Codec.dump(plan.metadata))
      else
        {:error, reason} ->
          Ash.Changeset.add_error(changeset, field: :work_object_id, message: inspect(reason))
      end
    end)
  end

  defp fetch_work_object(changeset, tenant, actor) do
    work_object_id = Ash.Changeset.get_attribute(changeset, :work_object_id)

    WorkObject
    |> Ash.get(
      work_object_id,
      actor: actor,
      authorize?: false,
      domain: Mezzanine.Work,
      tenant: tenant
    )
    |> case do
      {:ok, %WorkObject{} = work_object} -> {:ok, work_object}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_work_class(%WorkObject{} = work_object, tenant, actor) do
    WorkClass
    |> Ash.get(
      work_object.work_class_id,
      actor: actor,
      authorize?: false,
      domain: Mezzanine.Work,
      tenant: tenant
    )
    |> case do
      {:ok, %WorkClass{} = work_class} -> {:ok, work_class}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_policy_bundle(changeset, %WorkClass{} = work_class, tenant, actor) do
    policy_bundle_id =
      Ash.Changeset.get_attribute(changeset, :policy_bundle_id) || work_class.policy_bundle_id

    if is_nil(policy_bundle_id) do
      {:error, :missing_policy_bundle}
    else
      PolicyBundleResource
      |> Ash.get(
        policy_bundle_id,
        actor: actor,
        authorize?: false,
        domain: Mezzanine.Programs,
        tenant: tenant
      )
      |> case do
        {:ok, %PolicyBundleResource{} = policy_bundle} -> {:ok, policy_bundle}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp next_version(work_object_id, tenant, actor) do
    WorkPlan
    |> Ash.Query.for_read(:list_for_work_object, %{work_object_id: work_object_id})
    |> Ash.Query.set_tenant(tenant)
    |> Ash.read(actor: actor, authorize?: false, domain: Mezzanine.Work)
    |> case do
      {:ok, plans} -> {:ok, Enum.count(plans) + 1}
      {:error, reason} -> {:error, reason}
    end
  end

  defp to_model_work_object(%WorkObject{} = work_object, %WorkClass{} = work_class) do
    ModelWorkObject.new!(%{
      work_id: work_object.id,
      program_id: work_object.program_id,
      work_type: work_class.kind,
      title: work_object.title,
      payload: work_object.payload,
      normalized_payload: work_object.normalized_payload,
      status: work_object.status,
      policy_ref: work_class.policy_bundle_id,
      metadata: %{
        external_ref: work_object.external_ref,
        priority: work_object.priority,
        source_kind: work_object.source_kind,
        description: work_object.description
      }
    })
  end

  defp to_model_policy_bundle(%PolicyBundleResource{} = policy_bundle) do
    ModelPolicyBundle.new!(%{
      bundle_id: policy_bundle.id,
      source_ref: policy_bundle.source_ref,
      config: policy_bundle.config,
      prompt_template: policy_bundle.prompt_template,
      compiled_form: policy_bundle.compiled_form,
      metadata: policy_bundle.metadata
    })
  end
end
