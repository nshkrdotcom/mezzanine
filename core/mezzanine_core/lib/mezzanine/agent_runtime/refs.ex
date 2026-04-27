for {module, error} <- [
      {Mezzanine.AgentRuntime.SubjectRef, :invalid_subject_ref},
      {Mezzanine.AgentRuntime.RunRef, :invalid_run_ref},
      {Mezzanine.AgentRuntime.ExecutionRef, :invalid_execution_ref},
      {Mezzanine.AgentRuntime.WorkflowRef, :invalid_workflow_ref},
      {Mezzanine.AgentRuntime.TurnRef, :invalid_turn_ref},
      {Mezzanine.AgentRuntime.ActionRequestRef, :invalid_action_request_ref},
      {Mezzanine.AgentRuntime.ActionReceiptRef, :invalid_action_receipt_ref},
      {Mezzanine.AgentRuntime.SessionRef, :invalid_session_ref},
      {Mezzanine.AgentRuntime.WorkspaceRef, :invalid_workspace_ref},
      {Mezzanine.AgentRuntime.WorkerRef, :invalid_worker_ref},
      {Mezzanine.AgentRuntime.EventRef, :invalid_event_ref},
      {Mezzanine.AgentRuntime.EvidenceRef, :invalid_evidence_ref},
      {Mezzanine.AgentRuntime.AuthorityRef, :invalid_authority_ref},
      {Mezzanine.AgentRuntime.LowerRef, :invalid_lower_ref},
      {Mezzanine.AgentRuntime.MemoryRef, :invalid_memory_ref},
      {Mezzanine.AgentRuntime.ToolCatalogRef, :invalid_tool_catalog_ref}
    ] do
  defmodule module do
    @moduledoc "Opaque S0 substrate reference."
    alias Mezzanine.AgentRuntime.Support

    @error error
    @enforce_keys [:id]
    defstruct [:id, metadata: %{}]

    @type t :: %__MODULE__{id: String.t(), metadata: map()}

    @spec new(map() | keyword() | String.t() | t()) :: {:ok, t()} | {:error, atom()}
    def new(%__MODULE__{} = ref), do: {:ok, ref}
    def new(id) when is_binary(id), do: new(%{id: id})

    def new(attrs) do
      with {:ok, attrs} <- Support.normalize_attrs(attrs),
           :ok <- Support.reject_unsafe(attrs, @error),
           id <- Support.required(attrs, :id),
           true <- Support.safe_ref?(id),
           metadata <- Support.optional(attrs, :metadata, %{}),
           true <- is_map(metadata) do
        {:ok, %__MODULE__{id: id, metadata: metadata}}
      else
        _ -> {:error, @error}
      end
    end

    @spec new!(map() | keyword() | String.t() | t()) :: t()
    def new!(attrs) do
      case new(attrs) do
        {:ok, ref} -> ref
        {:error, reason} -> raise ArgumentError, "invalid #{inspect(__MODULE__)}: #{reason}"
      end
    end

    @spec dump(t()) :: map()
    def dump(%__MODULE__{} = ref),
      do: Support.drop_nil_values(%{"id" => ref.id, "metadata" => ref.metadata})
  end
end
