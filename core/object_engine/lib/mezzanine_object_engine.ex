defmodule MezzanineObjectEngine do
  @moduledoc """
  Neutral subject-ledger contract entrypoint for the Mezzanine rebuild.
  """

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.Objects,
      Mezzanine.Objects.SubjectRecord
    ]
  end
end
