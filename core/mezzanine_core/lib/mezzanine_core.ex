defmodule MezzanineCore do
  @moduledoc """
  Reusable business-semantics substrate for the Mezzanine workspace.
  """

  @doc """
  Returns the initial posture of the reusable core package.
  """
  @spec identity() :: map()
  def identity do
    %{
      role: :business_semantics_substrate,
      posture: :configurable,
      target: :distributed_ai_operations
    }
  end

  @doc """
  Lists the first configuration axes the generalized engine is expected to own.
  """
  @spec configuration_axes() :: [atom()]
  def configuration_axes do
    [:operating_model, :authority, :workflow, :compliance, :tenancy]
  end

  @doc """
  Lists the contract modules that freeze generator-assisted boundary posture.
  """
  @spec contract_modules() :: [module()]
  def contract_modules do
    [
      Mezzanine.Boundary.GenerationManifest,
      Mezzanine.Boundary.GenerationSpec
    ]
  end
end
