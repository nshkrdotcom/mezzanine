defmodule Mezzanine.Citadel.SubstrateIngress do
  @moduledoc """
  Mezzanine-side substrate-origin governance ingress.

  This module assembles Mezzanine run-intent context into the explicit
  substrate packet consumed by `Citadel.Governance.SubstrateIngress`.
  """

  alias Citadel.Governance.SubstrateIngress, as: CitadelSubstrateIngress
  alias Mezzanine.Citadel.SubstrateIngress.PacketBuilder
  alias Mezzanine.Intent.RunIntent

  @type compile_result :: {:ok, map()} | {:error, map()}

  @spec compile_run_intent(RunIntent.t(), map(), [map()], keyword()) :: compile_result()
  def compile_run_intent(%RunIntent{} = intent, attrs \\ %{}, policy_packs \\ [], opts \\ [])
      when is_map(attrs) and is_list(policy_packs) and is_list(opts) do
    with {:ok, packet} <- PacketBuilder.packet(intent, attrs) do
      CitadelSubstrateIngress.compile(packet, policy_packs, opts)
    end
  end
end
