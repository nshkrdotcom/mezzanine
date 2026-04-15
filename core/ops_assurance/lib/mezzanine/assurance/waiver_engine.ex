defmodule Mezzanine.Assurance.WaiverEngine do
  @moduledoc """
  Pure helper rules for waiver validity.
  """

  @spec active?(DateTime.t() | nil, DateTime.t()) :: boolean()
  def active?(expires_at, now \\ DateTime.utc_now())
  def active?(nil, _now), do: true

  def active?(%DateTime{} = expires_at, now) do
    DateTime.compare(expires_at, now) in [:gt, :eq]
  end
end
