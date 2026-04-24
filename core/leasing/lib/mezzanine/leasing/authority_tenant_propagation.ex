defmodule Mezzanine.Leasing.AuthorityTenantPropagation do
  @moduledoc """
  Mezzanine owner evidence for `AuthorityTenantPropagation.v1`.

  This module proves that owner facts from the governed authority path populated
  a real `Mezzanine.Leasing.AuthorizationScope` before lower-facts reads are
  allowed to proceed.
  """

  alias Mezzanine.Leasing.AuthorizationScope

  @contract_id "AuthorityTenantPropagation.v1"
  @tenant_id "tenant-phase6-m8"
  @execution_id "exec-phase6-m8"
  @installation_id "installation-phase6-m8"
  @budget_ref "budget://phase6/m8/local-no-spend"
  @no_bypass_scope_ref "no-bypass://phase6/m8/authority-tenant-budget"
  @lower_facts_propagation_ref "lower-facts://tenant-phase6-m8/run-phase6-m8"

  @required_owner_fields [
    :tenant_ref,
    :authority_decision_ref,
    :budget_ref,
    :no_bypass_scope_ref,
    :lineage_ref,
    :causation_ref,
    :idempotency_ref,
    :lower_facts_propagation_ref
  ]

  @type evidence :: %{
          contract_id: String.t(),
          authorization_scope: AuthorizationScope.t(),
          authorization_scope_ref: String.t(),
          authority_decision_ref: String.t(),
          tenant_ref: String.t(),
          budget_ref: String.t(),
          no_bypass_scope_ref: String.t(),
          lineage_ref: String.t(),
          causation_ref: String.t(),
          idempotency_ref: String.t(),
          lower_facts_propagation_ref: String.t(),
          forbidden_present?: false
        }

  @spec contract() :: map()
  def contract do
    %{
      id: @contract_id,
      owner: :mezzanine,
      consumes_owner_contract: @contract_id,
      required_owner_fields: @required_owner_fields
    }
  end

  @spec fixture() :: map()
  def fixture do
    %{
      tenant_ref: "tenant:tenant-phase6-m8",
      authority_decision_ref: "authority-decision:phase6-m8",
      budget_ref: @budget_ref,
      no_bypass_scope_ref: @no_bypass_scope_ref,
      lineage_ref: "lineage://phase6/m8/exec-phase6-m8",
      causation_ref: "causation://phase6/m8/request-phase6-m8",
      idempotency_ref: "idempotency://phase6/m8/tenant-phase6-m8/request-phase6-m8",
      lower_facts_propagation_ref: @lower_facts_propagation_ref,
      authorization_scope_attrs: %{
        tenant_id: @tenant_id,
        installation_id: @installation_id,
        installation_revision: 7,
        activation_epoch: 4,
        lease_epoch: 2,
        subject_id: "subject-phase6-m8",
        execution_id: @execution_id,
        trace_id: "trace-phase6-m8",
        actor_ref: %{id: "actor-phase6-m8"},
        authorized_at: ~U[2026-04-22 12:00:00Z]
      },
      lower_facts: %{
        tenant_id: @tenant_id,
        installation_id: @installation_id,
        propagation_ref: @lower_facts_propagation_ref,
        shortcut?: false
      }
    }
  end

  @spec authorization_scope_evidence(map() | keyword()) :: {:ok, evidence()} | {:error, term()}
  def authorization_scope_evidence(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    with {:ok, authority_decision_ref} <- required_ref(attrs, :authority_decision_ref),
         {:ok, tenant_ref} <- required_ref(attrs, :tenant_ref),
         {:ok, budget_ref} <- required_ref(attrs, :budget_ref),
         {:ok, no_bypass_scope_ref} <- required_ref(attrs, :no_bypass_scope_ref),
         {:ok, lineage_ref} <- required_ref(attrs, :lineage_ref),
         {:ok, causation_ref} <- required_ref(attrs, :causation_ref),
         {:ok, idempotency_ref} <- required_ref(attrs, :idempotency_ref),
         {:ok, lower_facts_ref} <- required_ref(attrs, :lower_facts_propagation_ref),
         {:ok, scope} <- authorization_scope(attrs),
         :ok <- tenant_ref_matches_scope(tenant_ref, scope),
         :ok <- lower_facts_matches(attrs, scope, lower_facts_ref) do
      {:ok,
       %{
         contract_id: @contract_id,
         authorization_scope: scope,
         authorization_scope_ref: authorization_scope_ref(scope),
         authority_decision_ref: authority_decision_ref,
         tenant_ref: tenant_ref,
         budget_ref: budget_ref,
         no_bypass_scope_ref: no_bypass_scope_ref,
         lineage_ref: lineage_ref,
         causation_ref: causation_ref,
         idempotency_ref: idempotency_ref,
         lower_facts_propagation_ref: lower_facts_ref,
         forbidden_present?: false
       }}
    end
  end

  def authorization_scope_evidence(_attrs), do: {:error, :invalid_authorization_scope_attrs}

  defp authorization_scope(%{authorization_scope_attrs: attrs})
       when is_map(attrs) or is_list(attrs) do
    AuthorizationScope.new(attrs)
  end

  defp authorization_scope(_attrs), do: {:error, :missing_authorization_scope}

  defp tenant_ref_matches_scope(tenant_ref, %AuthorizationScope{} = scope) do
    if tenant_ref == tenant_ref(scope.tenant_id) do
      :ok
    else
      {:error, {:cross_tenant_scope, scope.tenant_id}}
    end
  end

  defp lower_facts_matches(attrs, %AuthorizationScope{} = scope, lower_facts_ref) do
    case Map.get(attrs, :lower_facts) do
      %{shortcut?: true} ->
        {:error, {:forbidden_evidence, :direct_lower_shortcut_bypassing_authority}}

      %{tenant_id: tenant_id, propagation_ref: ^lower_facts_ref}
      when tenant_id == scope.tenant_id ->
        :ok

      %{tenant_id: tenant_id} when tenant_id != scope.tenant_id ->
        {:error, {:lower_facts_tenant_mismatch, tenant_id}}

      %{propagation_ref: propagation_ref} ->
        {:error, {:lower_facts_propagation_ref_mismatch, propagation_ref}}

      _other ->
        {:error, :missing_lower_facts_propagation_ref}
    end
  end

  defp authorization_scope_ref(%AuthorizationScope{} = scope) do
    "authorization-scope://" <> scope.tenant_id <> "/" <> scope.execution_id
  end

  defp required_ref(attrs, :authority_decision_ref) do
    required_ref(attrs, :authority_decision_ref, :missing_authority_decision_ref)
  end

  defp required_ref(attrs, :budget_ref), do: required_ref(attrs, :budget_ref, :missing_budget_ref)

  defp required_ref(attrs, :no_bypass_scope_ref) do
    required_ref(attrs, :no_bypass_scope_ref, :missing_no_bypass_scope_ref)
  end

  defp required_ref(attrs, :lower_facts_propagation_ref) do
    required_ref(attrs, :lower_facts_propagation_ref, :missing_lower_facts_propagation_ref)
  end

  defp required_ref(attrs, field), do: required_ref(attrs, field, {:missing_required_ref, field})

  defp required_ref(attrs, field, error) do
    case Map.get(attrs, field) do
      ref when is_binary(ref) and ref != "" -> {:ok, ref}
      _other -> {:error, error}
    end
  end

  defp tenant_ref(tenant_id), do: "tenant:" <> tenant_id
end
