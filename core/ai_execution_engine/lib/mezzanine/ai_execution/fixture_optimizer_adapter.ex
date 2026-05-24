defmodule Mezzanine.AIExecution.FixtureOptimizerAdapter do
  @moduledoc """
  Deterministic optimizer adapter for GEPA seam tests before live GEPA binding.
  """

  @behaviour Mezzanine.AIExecution.OptimizerAdapter

  alias GroundPlane.Boundary.Codec
  alias OuterBrain.ContextABI.Failure

  @impl true
  def propose(optimization_request, opts \\ [])

  def propose(optimization_request, _opts) when is_map(optimization_request) do
    with {:ok, tenant_ref} <- required(optimization_request, :tenant_ref),
         {:ok, objective_ref} <- required(optimization_request, :objective_ref),
         {:ok, promotion_policy_ref} <- required(optimization_request, :promotion_policy_ref),
         {:ok, trace_ref} <- required(optimization_request, :trace_ref),
         {:ok, candidate_source_refs} <- candidate_sources(optimization_request) do
      candidate_ref =
        %{
          tenant_ref: tenant_ref,
          objective_ref: objective_ref,
          promotion_policy_ref: promotion_policy_ref,
          candidate_source_refs: candidate_source_refs,
          trace_ref: trace_ref
        }
        |> Codec.digest()
        |> String.replace_prefix("sha256:", "optimization-candidate://")

      {:ok,
       [
         %{
           candidate_ref: candidate_ref,
           lineage_refs: candidate_source_refs,
           objective_score_ref: "objective-score://fixture/" <> suffix(candidate_ref),
           promotion_required?: true,
           trace_ref: trace_ref
         }
       ]}
    end
  end

  def propose(_optimization_request, _opts),
    do:
      failure("mezzanine.ai_execution.invalid_optimization_request.v1",
        safe_message: "optimization request is invalid"
      )

  defp candidate_sources(attrs) do
    case Map.get(attrs, :candidate_source_refs) || Map.get(attrs, "candidate_source_refs", []) do
      [first | _rest] = values when is_binary(first) ->
        {:ok, values}

      _other ->
        failure("mezzanine.ai_execution.missing_optimization_ref.v1",
          safe_message: "optimization request is missing candidate source refs",
          evidence_refs: ["field://candidate_source_refs"]
        )
    end
  end

  defp required(attrs, field) do
    case Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field)) do
      value when is_binary(value) and value != "" ->
        {:ok, value}

      _other ->
        failure("mezzanine.ai_execution.missing_optimization_ref.v1",
          safe_message: "optimization request is missing a required ref",
          evidence_refs: ["field://#{Atom.to_string(field)}"]
        )
    end
  end

  defp suffix(ref), do: ref |> String.split("/") |> List.last()

  defp failure(reason_code, opts) do
    {:ok, failure} =
      Failure.new(%{
        owner: :mezzanine,
        reason_code: reason_code,
        safe_message: Keyword.fetch!(opts, :safe_message),
        evidence_refs: Keyword.get(opts, :evidence_refs, [])
      })

    {:error, failure}
  end
end
