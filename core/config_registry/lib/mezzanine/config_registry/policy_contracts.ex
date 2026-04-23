defmodule Mezzanine.ConfigRegistry.PolicyContracts do
  @moduledoc """
  Phase 7 governed-memory policy contract helpers.
  """

  @granularity_precedence [
    :global,
    :tenant,
    :installation,
    :workspace,
    :agent,
    :actor_role,
    :time_window
  ]

  @transform_kinds [:identity, :redact, :anonymize, :paraphrase, :summarize, :quote_bound]
  @determinism [:deterministic, :stochastic]
  @degraded_behaviors [:fail_closed, :fail_empty, :fail_partial]
  @audit_levels [:minimal, :standard, :strict]
  @policy_kinds [:read, :write, :transform, :share_up, :promote, :invalidate]
  @target_tiers [:private, :shared, :governed]
  @known_keys [
    :access_projection_rule,
    :actor_role_ref,
    :agent_ref,
    :args,
    :audit_level,
    :auto_decide,
    :budget,
    :candidate_filter,
    :cascade_rule,
    :class,
    :composed_from,
    :contract_name,
    :dedupe_fn,
    :degraded_behavior,
    :determinism,
    :eligible?,
    :eligibility_fn,
    :evidence_requirements,
    :governed,
    :granularity_scope,
    :installation_ref,
    :kind,
    :left,
    :max_chars,
    :mode,
    :model_ref,
    :output_hash_anchor,
    :patterns,
    :pipeline,
    :policy_id,
    :private,
    :quorum_ref,
    :ranking_fn,
    :retention,
    :retention_override,
    :review_required,
    :right,
    :scope_kind,
    :share_up_eligibility,
    :shared,
    :target_scope_predicate,
    :target_tier,
    :tenant_ref,
    :tiers,
    :time_window_ref,
    :top_k_governed,
    :top_k_private,
    :top_k_shared,
    :transform_ref,
    :value,
    :version,
    :workspace_ref
  ]
  @known_atoms @granularity_precedence ++
                 @transform_kinds ++
                 @determinism ++
                 @degraded_behaviors ++
                 @audit_levels ++ @policy_kinds ++ @target_tiers
  @known_atom_by_string Map.new(@known_atoms, &{Atom.to_string(&1), &1})
  @known_key_by_string Map.new(@known_keys, &{Atom.to_string(&1), &1})

  @kind_by_module %{
    Mezzanine.ConfigRegistry.ReadPolicy => :read,
    Mezzanine.ConfigRegistry.WritePolicy => :write,
    Mezzanine.ConfigRegistry.TransformPolicy => :transform,
    Mezzanine.ConfigRegistry.ShareUpPolicy => :share_up,
    Mezzanine.ConfigRegistry.PromotePolicy => :promote,
    Mezzanine.ConfigRegistry.InvalidatePolicy => :invalidate
  }

  @spec granularity_precedence() :: [atom()]
  def granularity_precedence, do: @granularity_precedence

  @spec kind(struct()) :: atom()
  def kind(%{__struct__: module}) do
    Map.fetch!(@kind_by_module, module)
  end

  @spec policy_kind(atom() | String.t()) :: {:ok, atom()} | {:error, term()}
  def policy_kind(value) do
    with {:ok, kind} <- normalize_atom(value),
         true <- kind in @policy_kinds do
      {:ok, kind}
    else
      _error -> {:error, {:invalid_policy_kind, value}}
    end
  end

  @spec dump(struct()) :: map()
  def dump(%{__struct__: module} = policy) when is_map_key(@kind_by_module, module) do
    policy
    |> Map.from_struct()
    |> normalize_spec()
  end

  @spec build_contract(module(), String.t(), map() | keyword(), [atom()], function()) ::
          {:ok, struct()} | {:error, term()}
  def build_contract(module, contract_name, attrs, extra_fields, extra_builder)
      when is_atom(module) and is_binary(contract_name) and is_function(extra_builder, 1) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         {:ok, common} <- common_attrs(attrs),
         {:ok, extra} <- extra_builder.(attrs) do
      {:ok,
       struct!(
         module,
         [:contract_name, :policy_id, :version, :granularity_scope]
         |> Kernel.++(extra_fields)
         |> Map.new(fn field ->
           {field,
            Map.fetch!(Map.merge(Map.put(common, :contract_name, contract_name), extra), field)}
         end)
       )}
    end
  end

  @spec common_attrs(map()) :: {:ok, map()} | {:error, term()}
  def common_attrs(attrs) do
    with {:ok, policy_id} <- required_binary(attrs, :policy_id),
         {:ok, version} <- positive_integer(attrs, :version),
         {:ok, granularity_scope} <- granularity_scope(Map.get(attrs, :granularity_scope)) do
      {:ok,
       %{
         policy_id: policy_id,
         version: version,
         granularity_scope: granularity_scope
       }}
    end
  end

  @spec required_binary(map(), atom()) :: {:ok, String.t()} | {:error, term()}
  def required_binary(attrs, field) do
    case Map.get(attrs, field) do
      value when is_binary(value) ->
        value = String.trim(value)

        if value == "" do
          {:error, {:missing_required_field, field}}
        else
          {:ok, value}
        end

      _value ->
        {:error, {:missing_required_field, field}}
    end
  end

  @spec required_map(map(), atom()) :: {:ok, map()} | {:error, term()}
  def required_map(attrs, field) do
    case Map.get(attrs, field) do
      value when is_map(value) -> {:ok, normalize_spec(value)}
      _value -> {:error, {:missing_required_field, field}}
    end
  end

  @spec required_list(map(), atom()) :: {:ok, list()} | {:error, term()}
  def required_list(attrs, field) do
    case Map.get(attrs, field) do
      [_ | _] = value -> {:ok, value}
      _value -> {:error, {:missing_required_field, field}}
    end
  end

  @spec positive_integer(map(), atom()) :: {:ok, pos_integer()} | {:error, term()}
  def positive_integer(attrs, field) do
    case Map.get(attrs, field) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _value -> {:error, {:invalid_positive_integer, field}}
    end
  end

  @spec non_negative_integer(map(), atom()) :: {:ok, non_neg_integer()} | {:error, term()}
  def non_negative_integer(attrs, field) do
    case Map.get(attrs, field) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _value -> {:error, {:invalid_non_negative_integer, field}}
    end
  end

  @spec required_boolean(map(), atom()) :: {:ok, boolean()} | {:error, term()}
  def required_boolean(attrs, field) do
    case Map.get(attrs, field) do
      value when is_boolean(value) -> {:ok, value}
      _value -> {:error, {:missing_required_field, field}}
    end
  end

  @spec atom_one_of(map(), atom(), [atom()]) :: {:ok, atom()} | {:error, term()}
  def atom_one_of(attrs, field, allowed) do
    with {:ok, value} <- normalize_atom(Map.get(attrs, field)),
         true <- value in allowed do
      {:ok, value}
    else
      _error -> {:error, {:invalid_atom, field}}
    end
  end

  @spec granularity_scope(term()) :: {:ok, atom()} | {:error, term()}
  def granularity_scope(value) do
    with {:ok, normalized} <- normalize_atom(value),
         true <- normalized in @granularity_precedence do
      {:ok, normalized}
    else
      _error -> {:error, {:invalid_granularity_scope, value}}
    end
  end

  @spec narrower_scope(atom(), atom()) :: atom()
  def narrower_scope(left, right) do
    if precedence(left) >= precedence(right), do: left, else: right
  end

  @spec precedence(atom()) :: non_neg_integer()
  def precedence(scope) do
    Enum.find_index(@granularity_precedence, &(&1 == scope)) || 0
  end

  @spec transform_pipeline(map(), atom()) :: {:ok, [map()]} | {:error, term()}
  def transform_pipeline(attrs, field) do
    with {:ok, steps} <- required_list(attrs, field) do
      steps
      |> Enum.map(&normalize_transform_step/1)
      |> Enum.reduce_while({:ok, []}, fn
        {:ok, step}, {:ok, acc} -> {:cont, {:ok, [step | acc]}}
        {:error, reason}, _acc -> {:halt, {:error, reason}}
      end)
      |> case do
        {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @spec deterministic?(atom()) :: boolean()
  def deterministic?(determinism), do: determinism == :deterministic

  @spec determinism_from_pipeline([map()]) :: atom()
  def determinism_from_pipeline(pipeline) do
    if Enum.any?(pipeline, &(&1.kind in [:paraphrase, :summarize])) do
      :stochastic
    else
      :deterministic
    end
  end

  @spec identity_transform_ref?(term()) :: boolean()
  def identity_transform_ref?(value) when is_atom(value), do: value == :identity

  def identity_transform_ref?(value) when is_binary(value) do
    value
    |> String.downcase()
    |> then(&(&1 == "identity" or &1 == "transform://identity"))
  end

  def identity_transform_ref?(_value), do: false

  @spec normalize_spec(map()) :: map()
  def normalize_spec(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} ->
      {normalize_key(key), normalize_spec_value(nested)}
    end)
    |> Map.new()
  end

  defp normalize_spec_value(value) when is_map(value), do: normalize_spec(value)

  defp normalize_spec_value(values) when is_list(values),
    do: Enum.map(values, &normalize_spec_value/1)

  defp normalize_spec_value(value) when is_binary(value) do
    case normalize_atom(value) do
      {:ok, atom} when atom in @granularity_precedence -> atom
      {:ok, atom} when atom in @determinism -> atom
      {:ok, atom} when atom in @degraded_behaviors -> atom
      {:ok, atom} when atom in @audit_levels -> atom
      {:ok, atom} when atom in @transform_kinds -> atom
      _error -> value
    end
  end

  defp normalize_spec_value(value), do: value

  defp normalize_attrs(attrs) when is_list(attrs), do: {:ok, Map.new(attrs)}

  defp normalize_attrs(%{__struct__: _module} = attrs),
    do: {:ok, attrs |> Map.from_struct() |> normalize_spec()}

  defp normalize_attrs(attrs) when is_map(attrs), do: {:ok, normalize_spec(attrs)}
  defp normalize_attrs(_attrs), do: {:error, :invalid_policy_attrs}

  defp normalize_key(key) when is_atom(key), do: key

  defp normalize_key(key) when is_binary(key) do
    normalized =
      key
      |> String.trim()
      |> String.replace("-", "_")

    Map.get(@known_key_by_string, normalized, key)
  end

  defp normalize_transform_step(step) when is_atom(step),
    do: normalize_transform_step(%{kind: step})

  defp normalize_transform_step({kind, value}) when is_atom(kind),
    do: normalize_transform_step(Map.put(tuple_payload(kind, value), :kind, kind))

  defp normalize_transform_step(step) when is_map(step) do
    normalized = normalize_spec(step)

    with {:ok, kind} <- normalize_atom(Map.get(normalized, :kind)),
         true <- kind in @transform_kinds do
      {:ok, Map.put(normalized, :kind, kind)}
    else
      _error -> {:error, {:invalid_transform_step, step}}
    end
  end

  defp normalize_transform_step(step), do: {:error, {:invalid_transform_step, step}}

  defp tuple_payload(:redact, patterns), do: %{patterns: patterns}
  defp tuple_payload(:anonymize, args), do: %{args: args}
  defp tuple_payload(:paraphrase, args), do: %{args: args}
  defp tuple_payload(:summarize, args), do: %{args: args}
  defp tuple_payload(:quote_bound, max_chars), do: %{max_chars: max_chars}
  defp tuple_payload(_kind, value), do: %{value: value}

  defp normalize_atom(value) when is_atom(value), do: {:ok, value}

  defp normalize_atom(value) when is_binary(value) do
    normalized =
      value
      |> String.trim()
      |> String.replace("-", "_")

    cond do
      normalized == "" ->
        {:error, :invalid_atom}

      Map.has_key?(@known_atom_by_string, normalized) ->
        {:ok, Map.fetch!(@known_atom_by_string, normalized)}

      true ->
        {:error, :invalid_atom}
    end
  end

  defp normalize_atom(_value), do: {:error, :invalid_atom}
end

defmodule Mezzanine.ConfigRegistry.ReadPolicy do
  @moduledoc """
  Contract: `Platform.ReadPolicy.V1`.
  """

  alias Mezzanine.ConfigRegistry.PolicyContracts

  @contract_name "Platform.ReadPolicy.V1"
  @fields [
    :candidate_filter,
    :ranking_fn,
    :top_k_private,
    :top_k_shared,
    :top_k_governed,
    :transform_ref,
    :degraded_behavior,
    :audit_level
  ]

  defstruct [:contract_name, :policy_id, :version, :granularity_scope | @fields]

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, term()}
  def new(attrs) do
    PolicyContracts.build_contract(__MODULE__, @contract_name, attrs, @fields, fn attrs ->
      with {:ok, candidate_filter} <- PolicyContracts.required_map(attrs, :candidate_filter),
           {:ok, ranking_fn} <- PolicyContracts.required_binary(attrs, :ranking_fn),
           {:ok, top_k_private} <- PolicyContracts.non_negative_integer(attrs, :top_k_private),
           {:ok, top_k_shared} <- PolicyContracts.non_negative_integer(attrs, :top_k_shared),
           {:ok, top_k_governed} <- PolicyContracts.non_negative_integer(attrs, :top_k_governed),
           {:ok, transform_ref} <- PolicyContracts.required_binary(attrs, :transform_ref),
           {:ok, degraded_behavior} <-
             PolicyContracts.atom_one_of(
               attrs,
               :degraded_behavior,
               [:fail_closed, :fail_empty, :fail_partial]
             ),
           {:ok, audit_level} <-
             PolicyContracts.atom_one_of(attrs, :audit_level, [:minimal, :standard, :strict]) do
        {:ok,
         %{
           candidate_filter: candidate_filter,
           ranking_fn: ranking_fn,
           top_k_private: top_k_private,
           top_k_shared: top_k_shared,
           top_k_governed: top_k_governed,
           transform_ref: transform_ref,
           degraded_behavior: degraded_behavior,
           audit_level: audit_level
         }}
      end
    end)
  end
end

defmodule Mezzanine.ConfigRegistry.WritePolicy do
  @moduledoc """
  Contract: `Platform.WritePolicy.V1`.
  """

  alias Mezzanine.ConfigRegistry.PolicyContracts

  @contract_name "Platform.WritePolicy.V1"
  @fields [
    :target_tier,
    :retention,
    :transform_ref,
    :dedupe_fn,
    :share_up_eligibility,
    :audit_level
  ]

  defstruct [:contract_name, :policy_id, :version, :granularity_scope | @fields]

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, term()}
  def new(attrs) do
    PolicyContracts.build_contract(__MODULE__, @contract_name, attrs, @fields, fn attrs ->
      with {:ok, target_tier} <-
             PolicyContracts.atom_one_of(attrs, :target_tier, [:private, :shared, :governed]),
           {:ok, retention} <- PolicyContracts.required_map(attrs, :retention),
           {:ok, transform_ref} <- PolicyContracts.required_binary(attrs, :transform_ref),
           {:ok, dedupe_fn} <- PolicyContracts.required_binary(attrs, :dedupe_fn),
           {:ok, share_up_eligibility} <-
             PolicyContracts.required_map(attrs, :share_up_eligibility),
           {:ok, audit_level} <-
             PolicyContracts.atom_one_of(attrs, :audit_level, [:minimal, :standard, :strict]) do
        {:ok,
         %{
           target_tier: target_tier,
           retention: retention,
           transform_ref: transform_ref,
           dedupe_fn: dedupe_fn,
           share_up_eligibility: share_up_eligibility,
           audit_level: audit_level
         }}
      end
    end)
  end
end

defmodule Mezzanine.ConfigRegistry.TransformPolicy do
  @moduledoc """
  Contract: `Platform.TransformPolicy.V1`.
  """

  alias Mezzanine.ConfigRegistry.PolicyContracts

  @contract_name "Platform.TransformPolicy.V1"
  @fields [:pipeline, :determinism, :output_hash_anchor, :access_projection_rule]

  defstruct [:contract_name, :policy_id, :version, :granularity_scope | @fields]

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, term()}
  def new(attrs) do
    PolicyContracts.build_contract(__MODULE__, @contract_name, attrs, @fields, fn attrs ->
      with {:ok, pipeline} <- PolicyContracts.transform_pipeline(attrs, :pipeline),
           {:ok, determinism} <-
             PolicyContracts.atom_one_of(attrs, :determinism, [:deterministic, :stochastic]),
           {:ok, output_hash_anchor} <-
             PolicyContracts.required_binary(attrs, :output_hash_anchor),
           {:ok, access_projection_rule} <-
             PolicyContracts.required_map(attrs, :access_projection_rule) do
        {:ok,
         %{
           pipeline: pipeline,
           determinism: determinism,
           output_hash_anchor: output_hash_anchor,
           access_projection_rule: access_projection_rule
         }}
      end
    end)
  end

  @spec compose(t(), t()) :: {:ok, t()} | {:error, term()}
  def compose(%__MODULE__{} = left, %__MODULE__{} = right) do
    pipeline = left.pipeline ++ right.pipeline

    determinism =
      if left.determinism == :stochastic or right.determinism == :stochastic do
        :stochastic
      else
        PolicyContracts.determinism_from_pipeline(pipeline)
      end

    new(%{
      policy_id: "composed:#{left.policy_id}+#{right.policy_id}",
      version: max(left.version, right.version),
      granularity_scope:
        PolicyContracts.narrower_scope(left.granularity_scope, right.granularity_scope),
      pipeline: pipeline,
      determinism: determinism,
      output_hash_anchor: "composed:#{left.policy_id}+#{right.policy_id}",
      access_projection_rule: %{
        composed_from: [left.policy_id, right.policy_id],
        left: left.access_projection_rule,
        right: right.access_projection_rule
      }
    })
  end
end

defmodule Mezzanine.ConfigRegistry.ShareUpPolicy do
  @moduledoc """
  Contract: `Platform.ShareUpPolicy.V1`.
  """

  alias Mezzanine.ConfigRegistry.PolicyContracts

  @contract_name "Platform.ShareUpPolicy.V1"
  @fields [
    :transform_ref,
    :eligibility_fn,
    :target_scope_predicate,
    :access_projection_rule,
    :audit_level
  ]

  defstruct [:contract_name, :policy_id, :version, :granularity_scope | @fields]

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, term()}
  def new(attrs) do
    PolicyContracts.build_contract(__MODULE__, @contract_name, attrs, @fields, fn attrs ->
      with false <- PolicyContracts.identity_transform_ref?(Map.get(attrs, :transform_ref)),
           {:ok, transform_ref} <- PolicyContracts.required_binary(attrs, :transform_ref),
           {:ok, eligibility_fn} <- PolicyContracts.required_binary(attrs, :eligibility_fn),
           {:ok, target_scope_predicate} <-
             PolicyContracts.required_map(attrs, :target_scope_predicate),
           {:ok, access_projection_rule} <-
             PolicyContracts.required_map(attrs, :access_projection_rule),
           {:ok, audit_level} <-
             PolicyContracts.atom_one_of(attrs, :audit_level, [:minimal, :standard, :strict]) do
        {:ok,
         %{
           transform_ref: transform_ref,
           eligibility_fn: eligibility_fn,
           target_scope_predicate: target_scope_predicate,
           access_projection_rule: access_projection_rule,
           audit_level: audit_level
         }}
      else
        true -> {:error, :identity_share_up_forbidden}
        error -> error
      end
    end)
  end
end

defmodule Mezzanine.ConfigRegistry.PromotePolicy do
  @moduledoc """
  Contract: `Platform.PromotePolicy.V1`.
  """

  alias Mezzanine.ConfigRegistry.PolicyContracts

  @contract_name "Platform.PromotePolicy.V1"
  @fields [:review_required, :quorum_ref, :auto_decide, :evidence_requirements, :audit_level]

  defstruct [:contract_name, :policy_id, :version, :granularity_scope | @fields]

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, term()}
  def new(attrs) do
    PolicyContracts.build_contract(__MODULE__, @contract_name, attrs, @fields, fn attrs ->
      with {:ok, review_required} <- PolicyContracts.required_boolean(attrs, :review_required),
           {:ok, quorum_ref} <- PolicyContracts.required_binary(attrs, :quorum_ref),
           {:ok, auto_decide} <- PolicyContracts.required_boolean(attrs, :auto_decide),
           {:ok, evidence_requirements} <-
             PolicyContracts.required_list(attrs, :evidence_requirements),
           {:ok, audit_level} <-
             PolicyContracts.atom_one_of(attrs, :audit_level, [:minimal, :standard, :strict]) do
        {:ok,
         %{
           review_required: review_required,
           quorum_ref: quorum_ref,
           auto_decide: auto_decide,
           evidence_requirements:
             Enum.map(evidence_requirements, &PolicyContracts.normalize_spec/1),
           audit_level: audit_level
         }}
      end
    end)
  end
end

defmodule Mezzanine.ConfigRegistry.InvalidatePolicy do
  @moduledoc """
  Contract: `Platform.InvalidatePolicy.V1`.
  """

  alias Mezzanine.ConfigRegistry.PolicyContracts

  @contract_name "Platform.InvalidatePolicy.V1"
  @fields [:cascade_rule, :retention_override, :audit_level]

  defstruct [:contract_name, :policy_id, :version, :granularity_scope | @fields]

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, term()}
  def new(attrs) do
    PolicyContracts.build_contract(__MODULE__, @contract_name, attrs, @fields, fn attrs ->
      with {:ok, cascade_rule} <- PolicyContracts.required_map(attrs, :cascade_rule),
           {:ok, retention_override} <- PolicyContracts.required_map(attrs, :retention_override),
           {:ok, audit_level} <-
             PolicyContracts.atom_one_of(attrs, :audit_level, [:minimal, :standard, :strict]) do
        {:ok,
         %{
           cascade_rule: cascade_rule,
           retention_override: retention_override,
           audit_level: audit_level
         }}
      end
    end)
  end
end
