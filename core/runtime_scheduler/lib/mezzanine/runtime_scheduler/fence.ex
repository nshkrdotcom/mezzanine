defmodule Mezzanine.RuntimeScheduler.Fence do
  @moduledoc """
  Fence view projected from an installation lease.
  """

  alias Mezzanine.RuntimeScheduler.InstallationLease

  defstruct [:installation_id, :holder, :lease_id, :epoch, :compiled_pack_revision]

  @type t :: %__MODULE__{
          installation_id: String.t(),
          holder: String.t(),
          lease_id: String.t(),
          epoch: non_neg_integer(),
          compiled_pack_revision: pos_integer()
        }

  @spec from_lease(InstallationLease.t()) :: t()
  def from_lease(%InstallationLease{} = lease) do
    %__MODULE__{
      installation_id: lease.installation_id,
      holder: lease.holder,
      lease_id: lease.lease_id,
      epoch: lease.epoch,
      compiled_pack_revision: lease.compiled_pack_revision
    }
  end

  @spec newer_than?(t(), t()) :: boolean()
  def newer_than?(%__MODULE__{} = left, %__MODULE__{} = right) do
    left.epoch > right.epoch or
      (left.epoch == right.epoch and
         left.compiled_pack_revision > right.compiled_pack_revision)
  end
end
