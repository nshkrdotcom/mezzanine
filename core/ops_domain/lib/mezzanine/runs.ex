defmodule Mezzanine.Runs do
  @moduledoc """
  Durable execution-attempt truth for governed work.
  """

  use Ash.Domain, validate_config_inclusion?: false

  resources do
    resource Mezzanine.Runs.RunSeries
    resource Mezzanine.Runs.Run
    resource Mezzanine.Runs.RunGrant
    resource Mezzanine.Runs.RunArtifact
  end
end
