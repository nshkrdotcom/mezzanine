defmodule Mezzanine.IntegrationBridge.LinearGraphQLToolExecutor do
  @moduledoc """
  Lower-owned adapter for Linear GraphQL dynamic tools.

  This module translates the Codex dynamic-tool shape into the governed Linear
  connector operation. Query validation and provider execution remain below this
  bridge in Jido Integration.
  """

  alias Jido.Integration.V2.GovernedLowerDenial
  alias Mezzanine.IntegrationBridge.AuthorizedInvocation
  alias Mezzanine.IntegrationBridge.DirectRunDispatcher
  alias Mezzanine.IntegrationBridge.ProviderAuthorityAdmission

  @tool_name "linear_graphql"
  @operation "linear.graphql.execute"
  @supported_tools [@tool_name]
  @connector_ref "jido/connectors/linear"

  @spec execute_dynamic_tool(AuthorizedInvocation.t(), String.t(), term(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def execute_dynamic_tool(%AuthorizedInvocation{} = invocation, tool_name, arguments, opts \\ [])
      when is_binary(tool_name) and is_list(opts) do
    case normalize_tool_name(tool_name) do
      @tool_name ->
        execute_linear_graphql_tool(invocation, arguments, opts)

      unsupported ->
        {:ok,
         failed_result(
           unsupported,
           %{
             "error" => %{
               "message" => "Unsupported dynamic tool: #{inspect(tool_name)}.",
               "supportedTools" => @supported_tools
             }
           },
           opts
         )}
    end
  end

  @spec execute_linear_graphql_tool(AuthorizedInvocation.t(), term(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def execute_linear_graphql_tool(%AuthorizedInvocation{} = invocation, arguments, opts \\ [])
      when is_list(opts) do
    with {:ok, input} <- graphql_tool_input(arguments),
         dispatch_opts <- dispatch_opts(input, opts) do
      invocation
      |> DirectRunDispatcher.invoke_run_intent(dispatch_opts)
      |> dynamic_tool_result(opts)
    else
      {:error, payload} when is_map(payload) ->
        {:ok, failed_result(@tool_name, payload, opts)}
    end
  end

  defp graphql_tool_input(query) when is_binary(query) do
    query
    |> String.trim()
    |> case do
      "" ->
        input_error("`linear_graphql.query` must be a non-empty GraphQL query string.")

      trimmed ->
        {:ok, %{query: trimmed, variables: %{}}}
    end
  end

  defp graphql_tool_input(%{} = arguments) do
    query = value(arguments, :query)
    variables = value(arguments, :variables)

    with {:ok, query} <- normalize_query(query),
         {:ok, variables} <- normalize_variables(variables) do
      {:ok, %{query: query, variables: variables}}
    end
  end

  defp graphql_tool_input(_arguments) do
    input_error("`linear_graphql` arguments must be a query string or JSON object.")
  end

  defp normalize_query(query) when is_binary(query) do
    query
    |> String.trim()
    |> case do
      "" -> input_error("`linear_graphql.query` must be a non-empty GraphQL query string.")
      trimmed -> {:ok, trimmed}
    end
  end

  defp normalize_query(_query) do
    input_error("`linear_graphql.query` must be a non-empty GraphQL query string.")
  end

  defp normalize_variables(nil), do: {:ok, %{}}
  defp normalize_variables(%{} = variables), do: {:ok, Map.new(variables)}

  defp normalize_variables(_variables) do
    input_error("`linear_graphql.variables` must be a JSON object when provided.")
  end

  defp input_error(message) do
    {:error, %{"error" => %{"message" => message}}}
  end

  defp dispatch_opts(input, opts) do
    opts
    |> Keyword.put(:capability_id, @operation)
    |> Keyword.put(:input, input)
    |> Keyword.put_new(:lower_runtime_kind, :direct_connector)
    |> Keyword.put_new(:connector_ref, @connector_ref)
  end

  defp dynamic_tool_result({:ok, dispatch}, opts) when is_map(dispatch) do
    output = output_body(dispatch)
    success? = not graphql_error_body?(output)

    {:ok,
     base_result(opts)
     |> Map.merge(%{
       success?: success?,
       dynamic_tool_response: tool_response(success?, output),
       provider_request_sent?: true,
       provider_response_received?: true
     })
     |> authority_refs(dispatch)
     |> lower_refs(dispatch)
     |> generic_operation_refs(dispatch)
     |> maybe_put(:credential_redeemed?, credential_redeemed?(opts))}
  end

  defp dynamic_tool_result({:error, %GovernedLowerDenial{} = denial}, opts) do
    output = %{
      "error" => %{
        "message" => denial.reason,
        "class" => atomish_to_string(denial.denial_class)
      }
    }

    {:ok,
     failed_result(@tool_name, output, opts)
     |> Map.put(:lower_request_ref, denial.lower_request_ref)
     |> Map.put(:lower_denial_ref, denial.lower_denial_ref)}
  end

  defp dynamic_tool_result({:error, reason}, opts) do
    output = error_output(reason)

    {:ok,
     base_result(opts)
     |> Map.merge(%{
       success?: false,
       dynamic_tool_response: tool_response(false, output),
       provider_request_sent?: provider_request_sent?(reason),
       provider_response_received?: provider_response_received?(reason)
     })
     |> authority_refs(reason)
     |> lower_refs(reason)
     |> generic_operation_refs(reason)
     |> maybe_put(:credential_redeemed?, credential_redeemed?(opts))}
  end

  defp failed_result(tool_name, output, opts) do
    base_result(opts)
    |> Map.merge(%{
      tool_name: normalize_tool_name(tool_name),
      success?: false,
      dynamic_tool_response: tool_response(false, output),
      provider_request_sent?: false,
      provider_response_received?: false
    })
    |> maybe_put(:credential_redeemed?, credential_redeemed?(opts))
  end

  defp base_result(_opts) do
    %{
      operation: @operation,
      tool_name: @tool_name
    }
  end

  defp tool_response(success?, output) do
    encoded = Jason.encode!(output, pretty: true)

    %{
      "success" => success?,
      "output" => encoded,
      "contentItems" => [
        %{
          "type" => "inputText",
          "text" => encoded
        }
      ]
    }
  end

  defp output_body(dispatch) do
    output = value(dispatch, :output) || %{}

    if is_map(output) do
      output
      |> Map.drop([:auth_binding, "auth_binding"])
      |> stringify_keys()
      |> normalize_graphql_output()
    else
      %{"data" => output}
    end
  end

  defp normalize_graphql_output(%{"errors" => errors} = body)
       when is_list(errors) and errors != [],
       do: body

  defp normalize_graphql_output(%{"data" => data}) when is_map(data), do: %{"data" => data}

  defp normalize_graphql_output(%{"errors" => errors}) when is_map(errors),
    do: %{"errors" => errors}

  defp normalize_graphql_output(body), do: body

  defp graphql_error_body?(%{"errors" => errors}) when is_list(errors) and errors != [], do: true
  defp graphql_error_body?(%{errors: errors}) when is_list(errors) and errors != [], do: true
  defp graphql_error_body?(_body), do: false

  defp error_output(reason) do
    case upstream_body(reason) do
      %{} = body when map_size(body) > 0 ->
        stringify_keys(body)

      body when is_list(body) ->
        %{"errors" => body}

      body when is_binary(body) and body != "" ->
        case Jason.decode(body) do
          {:ok, decoded} -> decoded
          {:error, _reason} -> %{"error" => %{"message" => body}}
        end

      _missing ->
        %{
          "error" =>
            %{
              "message" => error_message(reason),
              "code" => value(reason, :code),
              "class" => value(reason, :class),
              "upstream_context" => stringify_keys(value(reason, :upstream_context) || %{})
            }
            |> compact_string_map()
        }
    end
  end

  defp upstream_body(reason) do
    reason
    |> value(:upstream_context)
    |> value(:body)
  end

  defp provider_request_sent?(reason) do
    context = value(reason, :upstream_context) || %{}

    present?(value(context, :http_status)) or present?(value(context, :provider_request_id)) or
      present?(value(context, :provider_code)) or present?(upstream_body(reason)) or
      present?(value(context, :graphql_errors))
  end

  defp provider_response_received?(reason) do
    context = value(reason, :upstream_context) || %{}

    present?(value(context, :http_status)) or present?(upstream_body(reason)) or
      present?(value(context, :graphql_errors))
  end

  defp lower_refs(result, source) do
    result
    |> maybe_put(:lower_request_ref, lower_request_ref(source))
    |> maybe_put(:lower_receipt_ref, lower_receipt_ref(source))
  end

  defp generic_operation_refs(result, source) do
    operation_receipt = value(source, :operation_receipt)

    result
    |> maybe_put(:operation_receipt, operation_receipt)
    |> maybe_put(:effect_request_ref, value(operation_receipt, :effect_request_ref))
    |> maybe_put(:connector_manifest_ref, value(operation_receipt, :connector_manifest_ref))
    |> maybe_put(:connector_manifest_hash, value(operation_receipt, :connector_manifest_hash))
    |> maybe_put(
      :capability_negotiation_ref,
      value(operation_receipt, :capability_negotiation_ref)
    )
    |> maybe_put(:evidence_profile_ref, value(operation_receipt, :evidence_profile_ref))
  end

  defp authority_refs(result, source) do
    Map.merge(result, ProviderAuthorityAdmission.result_fields(value(source, :authority_handoff)))
  end

  defp lower_request_ref(source) do
    source
    |> lower_receipt()
    |> value(:lower_request_ref)
    |> case do
      ref when is_binary(ref) and ref != "" ->
        ref

      _missing ->
        source
        |> lower_envelope()
        |> value(:lower_request_ref)
    end
  end

  defp lower_receipt_ref(source) do
    source
    |> lower_receipt()
    |> value(:lower_receipt_ref)
  end

  defp lower_receipt(source), do: value(source, :governed_lower_receipt)
  defp lower_envelope(source), do: value(source, :governed_lower_envelope)

  defp credential_redeemed?(opts) do
    case Keyword.fetch(opts, :credential_redeemed?) do
      {:ok, value} -> value
      :error -> nil
    end
  end

  defp normalize_tool_name("linear.graphql.execute"), do: @tool_name
  defp normalize_tool_name(tool_name) when is_binary(tool_name), do: tool_name

  defp error_message(%{message: message}) when is_binary(message), do: message
  defp error_message(%{"message" => message}) when is_binary(message), do: message
  defp error_message(%{reason: reason}), do: inspect(reason)
  defp error_message(%{"reason" => reason}), do: inspect(reason)
  defp error_message(reason), do: inspect(reason)

  defp stringify_keys(%_{} = struct), do: struct |> Map.from_struct() |> stringify_keys()

  defp stringify_keys(%{} = map) do
    map
    |> Enum.map(fn {key, value} -> {to_string(key), stringify_keys(value)} end)
    |> Map.new()
  end

  defp stringify_keys(values) when is_list(values), do: Enum.map(values, &stringify_keys/1)
  defp stringify_keys(value), do: value

  defp compact_string_map(map) do
    map
    |> Enum.reject(fn {_key, value} -> value in [nil, "", [], %{}] end)
    |> Map.new()
  end

  defp value(%_{} = struct, key), do: struct |> Map.from_struct() |> value(key)
  defp value(%{} = map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
  defp value(_value, _key), do: nil

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, _key, ""), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp present?(value) when value in [nil, "", [], %{}], do: false
  defp present?(_value), do: true

  defp atomish_to_string(value) when is_atom(value), do: Atom.to_string(value)
  defp atomish_to_string(value) when is_binary(value), do: value
  defp atomish_to_string(value), do: inspect(value)
end
