defmodule DiscordyGateway.Application.Stats do
  @moduledoc "Cluster-wide gateway figures for `GET /internal/v1/admin/stats` (P4)."

  alias DiscordyGateway.Application.Ports

  @spec snapshot() :: map()
  def snapshot do
    by_node = Ports.session_directory().count_by_node()

    %{
      "sessions_online" => by_node |> Map.values() |> Enum.sum(),
      "sessions_by_node" => by_node,
      "guild_processes" => Ports.server_directory().count(:guild),
      "voice_allocations" => Ports.voice_allocations().count()
    }
  end
end
