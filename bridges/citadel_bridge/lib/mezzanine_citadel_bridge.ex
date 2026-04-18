defmodule Mezzanine.CitadelBridge do
  @moduledoc """
  Compatibility-free substrate governance facade for Mezzanine run intents.
  """

  alias Mezzanine.Citadel.SubstrateIngress
  alias Mezzanine.Intent.RunIntent

  @spec compile_run_intent(RunIntent.t(), map(), [map()], keyword()) ::
          {:ok, map()} | {:error, map()}
  def compile_run_intent(%RunIntent{} = intent, attrs \\ %{}, policy_packs \\ [], opts \\ [])
      when is_map(attrs) and is_list(policy_packs) and is_list(opts) do
    SubstrateIngress.compile_run_intent(intent, attrs, policy_packs, opts)
  end

  @spec compile_submission(RunIntent.t(), map(), [map()], keyword()) ::
          {:ok, map()} | {:error, map()}
  def compile_submission(%RunIntent{} = intent, attrs \\ %{}, policy_packs \\ [], opts \\ [])
      when is_map(attrs) and is_list(policy_packs) and is_list(opts) do
    compile_run_intent(intent, attrs, policy_packs, opts)
  end
end
