defmodule Mezzanine.Pack.Diagnostics do
  @moduledoc false

  alias Mezzanine.Pack.ValidationError

  @spec errors([ValidationError.t()]) :: [ValidationError.t()]
  def errors(diagnostics) when is_list(diagnostics) do
    Enum.filter(diagnostics, &(&1.severity == :error))
  end
end
