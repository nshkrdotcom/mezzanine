defmodule Mezzanine.Core.GovernedEffects.Coordinator.Run do
  @moduledoc """
  In-memory lifecycle aggregate for one governed-effect execution.

  The durable owners arrive in later phases. This struct keeps the Phase 4
  coordinator pure and testable while preserving the fields those owners will
  persist or project.
  """

  alias Mezzanine.Core.GovernedEffects.AuthorityPacket
  alias Mezzanine.Core.GovernedEffects.EffectLog
  alias Mezzanine.Core.GovernedEffects.EffectReceipt
  alias Mezzanine.Core.GovernedEffects.GovernedEffect

  defstruct command: nil,
            effect: nil,
            log: nil,
            authority_packet: nil,
            authority_metadata: %{},
            invocation_envelope: nil,
            receipt: nil,
            reduced_facts: %{},
            projection: %{}

  @type t :: %__MODULE__{
          command: map() | keyword() | nil,
          effect: GovernedEffect.t() | nil,
          log: EffectLog.t() | nil,
          authority_packet: AuthorityPacket.t() | nil,
          authority_metadata: map(),
          invocation_envelope: map() | nil,
          receipt: EffectReceipt.t() | nil,
          reduced_facts: map(),
          projection: map()
        }
end
