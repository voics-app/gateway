defmodule DiscordyGateway.Infrastructure.Cluster.HordeServerDirectory do
  @moduledoc """
  Cluster-wide singleton registry for guild and DM-call processes on Horde: one registry plus a
  dynamic supervisor that places each process on exactly one node of the cluster.
  """

  @behaviour DiscordyGateway.Application.Ports.ServerDirectory

  @registry DiscordyGateway.Horde.Registry
  @supervisor DiscordyGateway.Horde.Supervisor

  @doc "Child specs of the registry and supervisor, for the composition root."
  @spec children() :: [Supervisor.child_spec()]
  def children do
    [
      {Horde.Registry, name: @registry, keys: :unique, members: :auto},
      {Horde.DynamicSupervisor, name: @supervisor, strategy: :one_for_one, members: :auto}
    ]
  end

  @impl true
  def via(key), do: {:via, Horde.Registry, {@registry, key}}

  @impl true
  def lookup(key) do
    case Horde.Registry.lookup(@registry, key) do
      [{pid, _value}] -> {:ok, pid}
      [] -> :error
    end
  end

  @impl true
  def ensure(key, child_spec) do
    case lookup(key) do
      {:ok, pid} -> {:ok, pid}
      :error -> start(child_spec)
    end
  end

  @impl true
  def count(kind) do
    @registry
    |> Horde.Registry.select([{{{:"$1", :_}, :_, :_}, [{:==, :"$1", kind}], [true]}])
    |> length()
  end

  defp start(child_spec) do
    case Horde.DynamicSupervisor.start_child(@supervisor, child_spec) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
      :ignore -> {:error, :ignored}
    end
  end
end
