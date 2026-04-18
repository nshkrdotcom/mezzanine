defmodule Mezzanine.ParallelBarrierCompletion do
  @moduledoc """
  Deduplicated child-completion row for a `ParallelBarrier`.
  """

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  @timestamps_opts [type: :utc_datetime_usec]

  @type t :: %__MODULE__{
          id: Ecto.UUID.t() | nil,
          barrier_id: Ecto.UUID.t() | nil,
          child_execution_id: Ecto.UUID.t() | nil,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  schema "parallel_barrier_completions" do
    field(:barrier_id, :binary_id)
    field(:child_execution_id, :binary_id)

    timestamps()
  end

  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(completion, attrs) do
    completion
    |> cast(attrs, [:barrier_id, :child_execution_id])
    |> validate_required([:barrier_id, :child_execution_id])
    |> unique_constraint([:barrier_id, :child_execution_id],
      name: :parallel_barrier_completions_barrier_child_index
    )
  end
end
