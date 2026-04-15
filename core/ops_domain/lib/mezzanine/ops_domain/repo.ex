defmodule Mezzanine.OpsDomain.Repo do
  use AshPostgres.Repo,
    otp_app: :mezzanine_ops_domain

  @impl true
  def installed_extensions do
    ["ash-functions", "pgcrypto"]
  end

  @impl true
  def min_pg_version do
    %Version{major: 16, minor: 0, patch: 0}
  end

  @impl true
  def prefer_transaction? do
    false
  end
end
