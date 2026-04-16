defmodule Mezzanine.Lifecycle.Evaluator do
  @moduledoc """
  Pure lifecycle transition evaluation against a compiled neutral pack.
  """

  alias Mezzanine.Lifecycle.SubjectSnapshot
  alias Mezzanine.Pack.{CompiledPack, SubjectContext}
  alias Mezzanine.Pack.Compiler.Helpers, as: H

  @type transition_result :: {:ok, map()} | {:error, :no_transition} | {:error, :guard_failed}

  @spec can_transition?(CompiledPack.t(), SubjectSnapshot.t(), CompiledPack.trigger_key()) ::
          transition_result()
  def can_transition?(%CompiledPack{} = compiled, %SubjectSnapshot{} = subject, trigger_key) do
    transitions =
      CompiledPack.transitions_for(compiled, subject.subject_kind, subject.lifecycle_state)

    trigger_key = H.canonicalize_trigger!(trigger_key)

    case Map.get(transitions, trigger_key) || fallback_transition(transitions, trigger_key) do
      nil ->
        {:error, :no_transition}

      transition ->
        if guard_passes?(Map.get(transition, :guard), subject) do
          {:ok, transition}
        else
          {:error, :guard_failed}
        end
    end
  end

  defp fallback_transition(transitions, {:execution_failed, recipe_ref, _failure_kind}) do
    Map.get(transitions, {:execution_failed, recipe_ref})
  end

  defp fallback_transition(_transitions, _trigger_key), do: nil

  defp guard_passes?(nil, _subject), do: true

  defp guard_passes?(%{module: module, function: function}, subject)
       when is_atom(module) and is_atom(function) do
    apply(module, function, [SubjectContext.from_snapshot(subject)]) == true
  rescue
    _error -> false
  end

  defp guard_passes?(_guard, _subject), do: false
end
