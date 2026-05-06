defmodule Mezzanine.AIRun.RunGraph do
  @moduledoc """
  Parent-child run graph refs for composed AI runs.
  """

  alias Mezzanine.AIRun.Envelope

  defmodule Edge do
    @moduledoc "Ref-only parent-child edge."
    @enforce_keys [
      :parent_run_ref,
      :child_run_ref,
      :tenant_ref,
      :idempotency_ref
    ]
    defstruct [
      :cancellation_ref,
      :retry_ref,
      :supersession_ref,
      :rollback_ref
      | @enforce_keys
    ]

    @type t :: %__MODULE__{}
  end

  @spec link_child(Envelope.t(), Envelope.t(), keyword() | map()) ::
          {:ok, Edge.t(), Envelope.t()} | {:error, term()}
  def link_child(parent, child, refs \\ [])

  def link_child(%Envelope{} = parent, %Envelope{} = child, refs) do
    if parent.tenant_ref == child.tenant_ref do
      idempotency_ref = ref(refs, :idempotency_ref) || child.idempotency_ref

      if present?(idempotency_ref) do
        edge = %Edge{
          parent_run_ref: parent.ai_run_ref,
          child_run_ref: child.ai_run_ref,
          tenant_ref: parent.tenant_ref,
          idempotency_ref: idempotency_ref,
          cancellation_ref: ref(refs, :cancellation_ref) || child.cancellation_ref,
          retry_ref: ref(refs, :retry_ref) || child.retry_ref,
          supersession_ref: ref(refs, :supersession_ref) || child.supersession_ref,
          rollback_ref: ref(refs, :rollback_ref) || child.rollback_ref
        }

        linked_child = %Envelope{
          child
          | parent_run_ref: parent.ai_run_ref,
            idempotency_ref: edge.idempotency_ref,
            cancellation_ref: edge.cancellation_ref,
            retry_ref: edge.retry_ref,
            supersession_ref: edge.supersession_ref,
            rollback_ref: edge.rollback_ref
        }

        {:ok, edge, linked_child}
      else
        {:error, {:missing_graph_ref, :idempotency_ref}}
      end
    else
      {:error, :tenant_mismatch}
    end
  end

  def link_child(_parent, _child, _refs), do: {:error, :invalid_run_graph_edge}

  defp ref(refs, key) when is_list(refs), do: Keyword.get(refs, key)

  defp ref(refs, key) when is_map(refs) do
    case Map.fetch(refs, key) do
      {:ok, value} -> value
      :error -> Map.get(refs, Atom.to_string(key))
    end
  end

  defp ref(_refs, _key), do: nil

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)
end
