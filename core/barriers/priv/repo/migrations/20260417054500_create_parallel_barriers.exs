defmodule Mezzanine.Execution.Repo.Migrations.CreateParallelBarriers do
  use Ecto.Migration

  def up do
    create table(:parallel_barriers, primary_key: false) do
      add :id, :uuid, primary_key: true, default: fragment("gen_random_uuid()")
      add :subject_id, :uuid, null: false
      add :barrier_key, :text, null: false
      add :join_step_ref, :text, null: false
      add :expected_children, :integer, null: false
      add :completed_children, :integer, null: false, default: 0
      add :status, :text, null: false, default: "open"
      add :trace_id, :text, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:parallel_barriers, [:subject_id, :barrier_key],
             name: :parallel_barriers_subject_barrier_key_index
           )

    create constraint(:parallel_barriers, :parallel_barriers_expected_children_check,
             check: "expected_children > 0"
           )

    create constraint(:parallel_barriers, :parallel_barriers_completed_children_check,
             check: "completed_children >= 0 AND completed_children <= expected_children"
           )

    create constraint(:parallel_barriers, :parallel_barriers_status_check,
             check: "status IN ('open', 'ready', 'closed')"
           )

    create table(:parallel_barrier_completions, primary_key: false) do
      add :id, :uuid, primary_key: true, default: fragment("gen_random_uuid()")
      add :barrier_id, :uuid, null: false
      add :child_execution_id, :uuid, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:parallel_barrier_completions, [:barrier_id, :child_execution_id],
             name: :parallel_barrier_completions_barrier_child_index
           )

    create index(:parallel_barrier_completions, [:barrier_id])

    execute(
      """
      CREATE FUNCTION prevent_parallel_barrier_expected_children_update()
      RETURNS trigger
      LANGUAGE plpgsql
      AS $$
      BEGIN
        IF NEW.expected_children IS DISTINCT FROM OLD.expected_children THEN
          RAISE EXCEPTION 'parallel_barriers.expected_children is immutable';
        END IF;

        RETURN NEW;
      END;
      $$;
      """,
      "DROP FUNCTION IF EXISTS prevent_parallel_barrier_expected_children_update()"
    )

    execute(
      """
      CREATE TRIGGER parallel_barriers_expected_children_immutable
      BEFORE UPDATE ON parallel_barriers
      FOR EACH ROW
      EXECUTE FUNCTION prevent_parallel_barrier_expected_children_update();
      """,
      "DROP TRIGGER IF EXISTS parallel_barriers_expected_children_immutable ON parallel_barriers"
    )
  end

  def down do
    execute(
      "DROP TRIGGER IF EXISTS parallel_barriers_expected_children_immutable ON parallel_barriers",
      "SELECT 1"
    )

    execute(
      "DROP FUNCTION IF EXISTS prevent_parallel_barrier_expected_children_update()",
      "SELECT 1"
    )

    drop_if_exists(index(:parallel_barrier_completions, [:barrier_id]))

    drop_if_exists(
      index(:parallel_barrier_completions, [:barrier_id, :child_execution_id],
        name: :parallel_barrier_completions_barrier_child_index
      )
    )

    drop_if_exists(table(:parallel_barrier_completions))

    drop_if_exists(constraint(:parallel_barriers, :parallel_barriers_status_check))
    drop_if_exists(constraint(:parallel_barriers, :parallel_barriers_completed_children_check))
    drop_if_exists(constraint(:parallel_barriers, :parallel_barriers_expected_children_check))

    drop_if_exists(
      index(:parallel_barriers, [:subject_id, :barrier_key],
        name: :parallel_barriers_subject_barrier_key_index
      )
    )

    drop_if_exists(table(:parallel_barriers))
  end
end
