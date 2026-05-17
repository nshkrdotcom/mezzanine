defmodule Mezzanine.Substrate.StructSupport do
  @moduledoc false

  defmacro __using__(opts) do
    required_fields = Keyword.fetch!(opts, :required)
    optional_fields = Keyword.get(opts, :optional, [])

    optional_field_names =
      Enum.map(optional_fields, fn
        {field, _default} -> field
        field -> field
      end)

    validators = Keyword.get(opts, :validate, [])

    quote bind_quoted: [
            optional_fields: optional_fields,
            optional_field_names: optional_field_names,
            required_fields: required_fields,
            validators: validators
          ] do
      alias Mezzanine.Substrate.Builder

      @substrate_required_fields required_fields
      @substrate_optional_field_names optional_field_names
      @substrate_validators validators

      @enforce_keys required_fields
      defstruct required_fields ++ optional_fields

      @type t :: %__MODULE__{}

      @spec fields() :: [atom()]
      def fields, do: @substrate_required_fields ++ @substrate_optional_field_names

      @spec required_fields() :: [atom()]
      def required_fields, do: @substrate_required_fields

      @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
      def new(attrs) do
        Builder.build(__MODULE__, attrs, @substrate_required_fields,
          validate: @substrate_validators
        )
      end
    end
  end
end
