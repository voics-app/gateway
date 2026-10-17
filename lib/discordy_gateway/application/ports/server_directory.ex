defmodule DiscordyGateway.Application.Ports.ServerDirectory do
  @moduledoc """
  Cluster-wide registry of singleton server processes: one per guild (`{:guild, id}`) and one
  per DM call (`{:dm_call, channel_id}`).
  """

  @type key :: {:guild | :dm_call, String.t()}

  @callback lookup(key()) :: {:ok, pid()} | :error
  @callback ensure(key(), child_spec :: Supervisor.child_spec()) :: {:ok, pid()} | {:error, term()}
  @callback count(kind :: :guild | :dm_call) :: non_neg_integer()
end
