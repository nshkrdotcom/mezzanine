defmodule Mezzanine.Leasing.Repo do
  use Ecto.Repo,
    otp_app: :mezzanine_leasing,
    adapter: Ecto.Adapters.Postgres
end
