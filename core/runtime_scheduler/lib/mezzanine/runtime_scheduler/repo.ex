defmodule Mezzanine.RuntimeScheduler.Repo do
  use Ecto.Repo,
    otp_app: :mezzanine_runtime_scheduler,
    adapter: Ecto.Adapters.Postgres
end
