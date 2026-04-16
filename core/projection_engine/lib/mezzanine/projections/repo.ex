defmodule Mezzanine.Projections.Repo do
  use AshPostgres.Repo,
    otp_app: :mezzanine_projection_engine,
    warn_on_missing_ash_functions?: false

  @impl true
  def installed_extensions do
    ["pgcrypto"]
  end

  @impl true
  def min_pg_version do
    %Version{major: 16, minor: 0, patch: 0}
  end
end
