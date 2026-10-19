defmodule DiscordyGateway.Application.Voice.Allocator do
  @moduledoc """
  P8 allocation: every user of a guild (or DM call) shares one voice node. An existing
  allocation is kept while its node is registered in etcd, even if the node is draining;
  otherwise the least-loaded non-draining node of the region is claimed.
  """

  alias DiscordyGateway.Application.Ports
  alias DiscordyGateway.Domain.Voice.VoiceNode

  @spec allocate(String.t(), String.t()) ::
          {:ok, VoiceNode.t()} | {:error, :no_voice_nodes | term()}
  def allocate(server_id, region) do
    store = Ports.voice_allocations()

    case store.get(server_id) do
      {:ok, node_id} -> existing_or_pick(store, server_id, node_id, region)
      :none -> pick(store, server_id, region)
    end
  end

  @doc "Drops the allocation of a server whose last voice user left."
  @spec release(String.t(), String.t()) :: :ok
  def release(server_id, node_id), do: Ports.voice_allocations().release(server_id, node_id)

  defp existing_or_pick(store, server_id, node_id, region) do
    case Ports.voice_nodes().get_node(node_id) do
      {:ok, node} ->
        {:ok, node}

      :error ->
        store.release(server_id, node_id)
        pick(store, server_id, region)
    end
  end

  defp pick(store, server_id, region) do
    with {:ok, node} <-
           VoiceNode.pick_least_loaded(Ports.voice_nodes().list_nodes(), region, store.drained()),
         {:ok, winner} <- store.claim(server_id, node.node_id) do
      resolve_winner(node, winner)
    end
  end

  defp resolve_winner(%VoiceNode{node_id: id} = node, id), do: {:ok, node}

  defp resolve_winner(_node, winner) do
    case Ports.voice_nodes().get_node(winner) do
      {:ok, node} -> {:ok, node}
      :error -> {:error, :no_voice_nodes}
    end
  end
end
