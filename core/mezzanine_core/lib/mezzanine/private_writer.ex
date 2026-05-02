defmodule Mezzanine.PrivateWriter.CommitRequest do
  @moduledoc "Request to commit accepted M2 candidate facts into private memory."

  alias Mezzanine.AgentRuntime.Support
  alias Mezzanine.PrivateWriter.AcceptancePolicy

  @required [
    :memory_commit_ref,
    :tenant_ref,
    :subject_ref,
    :run_ref,
    :turn_ref,
    :candidate_fact_refs,
    :source_observation_refs,
    :authority_decision_ref,
    :redaction_ref,
    :redaction_class,
    :claim_check_refs,
    :idempotency_key,
    :trace_id,
    :release_manifest_ref
  ]
  @optional [:candidate_facts, :supersedes_ref]
  @fields @required ++ @optional
  @redaction_classes [
    :claim_checked,
    :public_summary,
    :redacted
  ]
  @redaction_class_lookup Map.new(@redaction_classes, &{Atom.to_string(&1), &1})

  @enforce_keys @required
  defstruct @fields

  @type t :: %__MODULE__{}

  @spec new(t() | map() | keyword()) :: {:ok, t()} | {:error, atom()}
  def new(%__MODULE__{} = request), do: {:ok, request}
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- reject_unknown(attrs),
         :ok <- Support.reject_unsafe(attrs, :invalid_private_commit_request),
         true <- required_refs?(attrs, refs_required()),
         true <- non_empty_list_of_refs?(Support.required(attrs, :candidate_fact_refs)),
         true <- non_empty_list_of_refs?(Support.required(attrs, :source_observation_refs)),
         true <- non_empty_list_of_refs?(Support.required(attrs, :claim_check_refs)),
         true <- present_string?(Support.required(attrs, :idempotency_key)),
         redaction_class <- normalize_atom(Support.required(attrs, :redaction_class)),
         true <- redaction_class in @redaction_classes,
         candidate_facts <- Support.optional(attrs, :candidate_facts, []),
         :ok <- AcceptancePolicy.validate_candidate_facts(candidate_facts) do
      {:ok,
       struct!(
         __MODULE__,
         attrs
         |> values(@fields)
         |> Map.put(:redaction_class, redaction_class)
         |> Map.put(:candidate_facts, candidate_facts)
       )}
    else
      _ -> {:error, :invalid_private_commit_request}
    end
  end

  def new(_attrs), do: {:error, :invalid_private_commit_request}
  def new!(attrs), do: bang(new(attrs))

  @spec dump(t()) :: map()
  def dump(%__MODULE__{} = request), do: dump_struct(request)

  defp refs_required do
    [
      :memory_commit_ref,
      :tenant_ref,
      :subject_ref,
      :run_ref,
      :turn_ref,
      :authority_decision_ref,
      :redaction_ref,
      :trace_id,
      :release_manifest_ref
    ]
  end

  defp required_refs?(attrs, keys),
    do: Enum.all?(keys, &(Support.required(attrs, &1) |> Support.safe_ref?()))

  defp non_empty_list_of_refs?(values),
    do: is_list(values) and values != [] and Enum.all?(values, &Support.safe_ref?/1)

  defp present_string?(value), do: is_binary(value) and String.trim(value) != ""

  defp normalize_atom(value) when is_binary(value),
    do: Map.get(@redaction_class_lookup, value, value)

  defp normalize_atom(value), do: value
  defp values(attrs, fields), do: Map.new(fields, &{&1, Support.optional(attrs, &1)})
  defp bang({:ok, value}), do: value
  defp bang({:error, reason}), do: raise(ArgumentError, Atom.to_string(reason))

  defp reject_unknown(attrs) do
    allowed = MapSet.new(Enum.flat_map(@fields, &[&1, Atom.to_string(&1)]))

    if Enum.all?(Map.keys(attrs), &MapSet.member?(allowed, &1)),
      do: :ok,
      else: {:error, :unknown_fields}
  end

  defp dump_struct(%_{} = struct),
    do: struct |> Map.from_struct() |> Support.dump_value() |> Support.drop_nil_values()
end

defmodule Mezzanine.PrivateWriter.M7AProof do
  @moduledoc "Deterministic `m7a.v1` proof for a private memory commit."

  alias Mezzanine.AgentRuntime.Support

  @required [
    :proof_ref,
    :memory_commit_ref,
    :candidate_fact_refs,
    :source_observation_refs,
    :authority_decision_ref,
    :redaction_manifest_ref,
    :integrity_hash,
    :tenant_ref,
    :trace_id,
    :release_manifest_ref
  ]
  @enforce_keys @required
  defstruct @required

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, atom()}
  def new(%__MODULE__{} = proof), do: {:ok, proof}
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- reject_unknown(attrs),
         true <-
           required_refs?(attrs, [
             :proof_ref,
             :memory_commit_ref,
             :authority_decision_ref,
             :redaction_manifest_ref,
             :tenant_ref,
             :trace_id,
             :release_manifest_ref
           ]),
         true <- non_empty_list_of_refs?(Support.required(attrs, :candidate_fact_refs)),
         true <- non_empty_list_of_refs?(Support.required(attrs, :source_observation_refs)),
         integrity_hash <- Support.required(attrs, :integrity_hash),
         true <- is_binary(integrity_hash) and String.starts_with?(integrity_hash, "sha256:") do
      {:ok, struct!(__MODULE__, values(attrs, @required))}
    else
      _ -> {:error, :invalid_m7a_proof}
    end
  end

  def new(_attrs), do: {:error, :invalid_m7a_proof}
  def new!(attrs), do: bang(new(attrs))

  @spec dump(t()) :: map()
  def dump(%__MODULE__{} = proof), do: dump_struct(proof)

  @spec build(Mezzanine.PrivateWriter.CommitRequest.t()) :: t()
  def build(%Mezzanine.PrivateWriter.CommitRequest{} = request) do
    integrity_payload = %{
      schema_ref: "m7a.v1",
      memory_commit_ref: request.memory_commit_ref,
      candidate_fact_refs: request.candidate_fact_refs,
      source_observation_refs: request.source_observation_refs,
      authority_decision_ref: request.authority_decision_ref,
      redaction_manifest_ref: request.redaction_ref,
      tenant_ref: request.tenant_ref,
      trace_id: request.trace_id,
      release_manifest_ref: request.release_manifest_ref,
      claim_check_refs: request.claim_check_refs
    }

    new!(%{
      proof_ref: "m7a-proof://#{Mezzanine.PrivateWriter.ref_suffix(request.memory_commit_ref)}",
      memory_commit_ref: request.memory_commit_ref,
      candidate_fact_refs: request.candidate_fact_refs,
      source_observation_refs: request.source_observation_refs,
      authority_decision_ref: request.authority_decision_ref,
      redaction_manifest_ref: request.redaction_ref,
      integrity_hash: Mezzanine.PrivateWriter.integrity_hash(integrity_payload),
      tenant_ref: request.tenant_ref,
      trace_id: request.trace_id,
      release_manifest_ref: request.release_manifest_ref
    })
  end

  defp required_refs?(attrs, keys),
    do: Enum.all?(keys, &(Support.required(attrs, &1) |> Support.safe_ref?()))

  defp non_empty_list_of_refs?(values),
    do: is_list(values) and values != [] and Enum.all?(values, &Support.safe_ref?/1)

  defp reject_unknown(attrs) do
    allowed = MapSet.new(Enum.flat_map(@required, &[&1, Atom.to_string(&1)]))

    if Enum.all?(Map.keys(attrs), &MapSet.member?(allowed, &1)),
      do: :ok,
      else: {:error, :unknown_fields}
  end

  defp values(attrs, fields), do: Map.new(fields, &{&1, Support.optional(attrs, &1)})
  defp bang({:ok, value}), do: value
  defp bang({:error, reason}), do: raise(ArgumentError, Atom.to_string(reason))

  defp dump_struct(%_{} = struct),
    do: struct |> Map.from_struct() |> Support.dump_value() |> Support.drop_nil_values()
end

defmodule Mezzanine.PrivateWriter.PrivateCommit do
  @moduledoc "Durable private-memory commit row with deterministic proof refs."

  alias Mezzanine.AgentRuntime.Support

  @fields [
    :memory_commit_ref,
    :tenant_ref,
    :subject_ref,
    :run_ref,
    :turn_ref,
    :candidate_fact_refs,
    :source_observation_refs,
    :authority_decision_ref,
    :redaction_ref,
    :redaction_class,
    :claim_check_refs,
    :idempotency_key,
    :trace_id,
    :release_manifest_ref,
    :supersedes_ref,
    :commit_lsn,
    :commit_hlc,
    :m7a_proof_refs,
    :recall_refs
  ]

  @enforce_keys @fields -- [:supersedes_ref]
  defstruct @fields

  @type t :: %__MODULE__{}

  @spec from_request(
          Mezzanine.PrivateWriter.CommitRequest.t(),
          Mezzanine.PrivateWriter.M7AProof.t()
        ) :: t()
  def from_request(
        %Mezzanine.PrivateWriter.CommitRequest{} = request,
        %Mezzanine.PrivateWriter.M7AProof{} = proof
      ) do
    suffix = Mezzanine.PrivateWriter.ref_suffix(request.memory_commit_ref)

    %__MODULE__{
      memory_commit_ref: request.memory_commit_ref,
      tenant_ref: request.tenant_ref,
      subject_ref: request.subject_ref,
      run_ref: request.run_ref,
      turn_ref: request.turn_ref,
      candidate_fact_refs: request.candidate_fact_refs,
      source_observation_refs: request.source_observation_refs,
      authority_decision_ref: request.authority_decision_ref,
      redaction_ref: request.redaction_ref,
      redaction_class: request.redaction_class,
      claim_check_refs: request.claim_check_refs,
      idempotency_key: request.idempotency_key,
      trace_id: request.trace_id,
      release_manifest_ref: request.release_manifest_ref,
      supersedes_ref: request.supersedes_ref,
      commit_lsn: "private-lsn://#{suffix}",
      commit_hlc: "private-hlc://#{suffix}",
      m7a_proof_refs: [proof.proof_ref],
      recall_refs: ["recall://private/#{suffix}"]
    }
  end

  @spec dump(t()) :: map()
  def dump(%__MODULE__{} = commit), do: dump_struct(commit)

  defp dump_struct(%_{} = struct),
    do: struct |> Map.from_struct() |> Support.dump_value() |> Support.drop_nil_values()
end

defmodule Mezzanine.PrivateWriter.CommitReceipt do
  @moduledoc "Result returned by `Mezzanine.PrivateWriter.commit/2`."

  alias Mezzanine.AgentRuntime.Support
  alias Mezzanine.PrivateWriter.{M7AProof, PrivateCommit}

  @enforce_keys [:receipt_ref, :private_commit, :m7a_proof, :idempotency_key, :status]
  defstruct [:receipt_ref, :private_commit, :m7a_proof, :idempotency_key, :status]

  @type t :: %__MODULE__{}

  @spec new(PrivateCommit.t(), M7AProof.t()) :: t()
  def new(%PrivateCommit{} = commit, %M7AProof{} = proof) do
    %__MODULE__{
      receipt_ref:
        "private-commit-receipt://#{Mezzanine.PrivateWriter.ref_suffix(commit.memory_commit_ref)}",
      private_commit: commit,
      m7a_proof: proof,
      idempotency_key: commit.idempotency_key,
      status: :committed
    }
  end

  @spec dump(t()) :: map()
  def dump(%__MODULE__{} = receipt),
    do: receipt |> Map.from_struct() |> Support.dump_value() |> Support.drop_nil_values()
end

defmodule Mezzanine.PrivateWriter.AcceptancePolicy do
  @moduledoc "Mezzanine acceptance policy for OuterBrain candidate fact proposals."

  alias Mezzanine.AgentRuntime.Support

  @required_candidate_fields [
    :candidate_fact_ref,
    :redaction_ref,
    :redaction_class,
    :claim_check_refs,
    :source_observation_ref,
    :evidence_ref,
    :trace_id
  ]
  @redaction_classes [
    :claim_checked,
    :public_summary,
    :redacted,
    "claim_checked",
    "public_summary",
    "redacted"
  ]

  @spec validate_candidate_facts([map() | struct()]) :: :ok | {:error, atom()}
  def validate_candidate_facts([]), do: :ok

  def validate_candidate_facts(candidate_facts) when is_list(candidate_facts) do
    if Enum.all?(candidate_facts, &valid_candidate_fact?/1),
      do: :ok,
      else: {:error, :invalid_candidate_fact_proposal}
  end

  def validate_candidate_facts(_candidate_facts), do: {:error, :invalid_candidate_fact_proposal}

  defp valid_candidate_fact?(fact) do
    with {:ok, attrs} <- Support.normalize_attrs(fact),
         :ok <- Support.reject_unsafe(attrs, :invalid_candidate_fact_proposal),
         true <- Enum.all?(@required_candidate_fields, &present_required?(attrs, &1)),
         true <- Support.required(attrs, :redaction_class) in @redaction_classes,
         claim_check_refs <- Support.required(attrs, :claim_check_refs),
         true <-
           is_list(claim_check_refs) and claim_check_refs != [] and
             Enum.all?(claim_check_refs, &Support.safe_ref?/1) do
      true
    else
      _ -> false
    end
  end

  defp present_required?(attrs, :claim_check_refs),
    do: is_list(Support.required(attrs, :claim_check_refs))

  defp present_required?(attrs, :redaction_class),
    do: Support.required(attrs, :redaction_class) in @redaction_classes

  defp present_required?(attrs, key), do: Support.required(attrs, key) |> Support.safe_ref?()
end

defmodule Mezzanine.PrivateWriter do
  @moduledoc """
  Sole Mezzanine writer for `M^private` memory.

  The writer accepts only M2-originated commit requests and returns a
  deterministic `m7a.v1` proof. This first implementation uses an ETS-backed
  in-process store for local/offline proofs; the public contract keeps the
  idempotency and integrity semantics durable-store ready.
  """

  alias __MODULE__.{CommitReceipt, CommitRequest, M7AProof, PrivateCommit}

  @table __MODULE__.Store
  @m2_callers [:m2_agent_loop, "m2://agent-loop"]

  @spec commit(CommitRequest.t() | map() | keyword(), keyword()) ::
          {:ok, CommitReceipt.t()} | {:error, atom()}
  def commit(request, opts \\ []) do
    with :ok <- authorize_caller(Keyword.get(opts, :caller)),
         {:ok, request} <- CommitRequest.new(request) do
      commit_request(request)
    end
  end

  @spec recall_refs(CommitReceipt.t() | PrivateCommit.t()) :: [String.t()]
  def recall_refs(%CommitReceipt{private_commit: %PrivateCommit{} = commit}),
    do: commit.recall_refs

  def recall_refs(%PrivateCommit{} = commit), do: commit.recall_refs

  @spec reset!() :: :ok
  def reset! do
    ensure_table!()
    :ets.delete_all_objects(@table)
    :ok
  end

  @doc false
  def integrity_hash(payload) do
    "sha256:" <>
      (payload
       |> canonicalize()
       |> :erlang.term_to_binary()
       |> then(&:crypto.hash(:sha256, &1))
       |> Base.encode16(case: :lower))
  end

  @doc false
  def ref_suffix(ref) when is_binary(ref) do
    ref
    |> String.replace(~r/[^A-Za-z0-9]+/, "-")
    |> String.trim("-")
  end

  def ref_suffix(ref), do: ref |> to_string() |> ref_suffix()

  defp commit_request(%CommitRequest{} = request) do
    ensure_table!()

    proof = M7AProof.build(request)
    private_commit = PrivateCommit.from_request(request, proof)
    receipt = CommitReceipt.new(private_commit, proof)
    request_hash = integrity_hash(CommitRequest.dump(request))

    case :ets.lookup(@table, request.memory_commit_ref) do
      [] ->
        true = :ets.insert(@table, {request.memory_commit_ref, request_hash, receipt})
        {:ok, receipt}

      [{_ref, ^request_hash, %CommitReceipt{} = existing}] ->
        {:ok, existing}

      [{_ref, _other_hash, _existing}] ->
        {:error, :divergent_private_commit}
    end
  end

  defp authorize_caller(caller) when caller in @m2_callers, do: :ok
  defp authorize_caller(_caller), do: {:error, :private_writer_requires_m2_caller}

  defp ensure_table! do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [:named_table, :public, {:read_concurrency, true}])
        :ok

      _tid ->
        :ok
    end
  end

  defp canonicalize(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp canonicalize(%_{} = value), do: value |> Map.from_struct() |> canonicalize()
  defp canonicalize(values) when is_list(values), do: Enum.map(values, &canonicalize/1)

  defp canonicalize(%{} = map) do
    map
    |> Enum.map(fn {key, value} -> {to_string(key), canonicalize(value)} end)
    |> Enum.sort_by(fn {key, _value} -> key end)
  end

  defp canonicalize(value) when is_atom(value) and not is_boolean(value) and not is_nil(value),
    do: Atom.to_string(value)

  defp canonicalize(value), do: value
end
