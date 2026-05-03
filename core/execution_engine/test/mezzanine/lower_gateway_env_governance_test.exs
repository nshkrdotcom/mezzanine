defmodule Mezzanine.LowerGatewayEnvGovernanceTest do
  use ExUnit.Case, async: false

  alias Mezzanine.LowerGateway

  defmodule EnvConfiguredGateway do
    @behaviour LowerGateway

    @impl true
    def dispatch(_claim), do: {:accepted, %{provider_token_source: "env"}}

    @impl true
    def lookup_submission(_submission_dedupe_key, _tenant_id), do: :never_seen

    @impl true
    def fetch_execution_outcome(_execution_lookup, _tenant_id), do: :pending

    @impl true
    def request_cancel(_submission_ref, _tenant_id, _reason), do: {:error, :not_used}
  end

  defmodule ExplicitGateway do
    @behaviour LowerGateway

    @impl true
    def dispatch(_claim), do: {:accepted, %{provider_token_source: "authority_packet"}}

    @impl true
    def lookup_submission(_submission_dedupe_key, _tenant_id), do: :never_seen

    @impl true
    def fetch_execution_outcome(_execution_lookup, _tenant_id), do: :pending

    @impl true
    def request_cancel(_submission_ref, _tenant_id, _reason), do: {:error, :not_used}
  end

  setup do
    previous = Application.get_env(:mezzanine_execution_engine, :lower_gateway_impl)
    Application.put_env(:mezzanine_execution_engine, :lower_gateway_impl, EnvConfiguredGateway)

    on_exit(fn ->
      if previous do
        Application.put_env(:mezzanine_execution_engine, :lower_gateway_impl, previous)
      else
        Application.delete_env(:mezzanine_execution_engine, :lower_gateway_impl)
      end
    end)
  end

  test "governed dispatch ignores application-configured lower gateway modules" do
    assert {:error, {:lower_gateway_unconfigured, Mezzanine.LowerGateway.Unconfigured}} =
             LowerGateway.dispatch(governed_claim())
  end

  test "explicit governed lower gateway module is accepted" do
    assert {:accepted, %{provider_token_source: "authority_packet"}} =
             LowerGateway.dispatch(
               Map.put(governed_claim(), :lower_gateway_impl, ExplicitGateway)
             )
  end

  defp governed_claim do
    %{
      execution_id: Ecto.UUID.generate(),
      tenant_id: "tenant-env-governance",
      installation_id: "installation-env-governance",
      subject_id: Ecto.UUID.generate(),
      trace_id: "trace-env-governance",
      causation_id: "cause-env-governance",
      submission_dedupe_key: "tenant-env-governance:lower:1",
      compiled_pack_revision: 1,
      binding_snapshot: %{
        "connector_binding_ref" => "connector-binding://authority/linear",
        "credential_lease_ref" => "credential-lease://lease/redacted"
      },
      dispatch_envelope: %{
        "authority_decision_ref" => "authority-decision://allow/1",
        "credential_lease_ref" => "credential-lease://lease/redacted"
      }
    }
  end
end
