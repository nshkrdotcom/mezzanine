defmodule Mezzanine.Evidence do
  @moduledoc """
  Durable evidence and audit truth for governed work.
  """

  use Ash.Domain, validate_config_inclusion?: false

  resources do
    resource Mezzanine.Evidence.EvidenceBundle
    resource Mezzanine.Evidence.EvidenceItem
    resource Mezzanine.Evidence.AuditEvent
    resource Mezzanine.Evidence.TimelineProjection
  end
end
