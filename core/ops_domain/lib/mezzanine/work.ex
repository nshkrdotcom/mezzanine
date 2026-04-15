defmodule Mezzanine.Work do
  @moduledoc """
  Durable work-definition and work-lifecycle truth.
  """

  use Ash.Domain, validate_config_inclusion?: false

  resources do
    resource Mezzanine.Work.WorkClass
    resource Mezzanine.Work.WorkObject
    resource Mezzanine.Work.WorkPlan
  end
end
