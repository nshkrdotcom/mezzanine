defmodule Mezzanine.Repo do
  @moduledoc "Canonical Postgres repository for Mezzanine-owned run truth."

  use Ecto.Repo,
    otp_app: :mezzanine_core,
    adapter: Ecto.Adapters.Postgres
end
