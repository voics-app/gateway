defmodule DiscordyGateway.Domain.Presence do
  @moduledoc """
  Presence aggregated across all of a user's sessions, Discord-style: dnd beats online beats
  idle; invisible sessions look offline to everyone else.
  """

  @priority %{"dnd" => 3, "online" => 2, "idle" => 1}

  @type session_presence :: %{
          status: String.t(),
          activities: [map()],
          afk: boolean(),
          platform: String.t()
        }
  @type aggregate :: %{status: String.t(), activities: [map()], client_status: %{String.t() => String.t()}}

  @spec aggregate([session_presence()]) :: aggregate()
  def aggregate(sessions) do
    visible = Enum.filter(sessions, &Map.has_key?(@priority, &1.status))

    case Enum.max_by(visible, &@priority[&1.status], fn -> nil end) do
      nil -> %{status: "offline", activities: [], client_status: %{}}
      best -> %{status: best.status, activities: best.activities, client_status: client_status(visible)}
    end
  end

  @doc "Maps IDENTIFY properties to a Discord client_status platform."
  @spec platform_for(%{os: String.t(), client: String.t()}) :: String.t()
  def platform_for(%{os: os}) when os in ["android", "ios"], do: "mobile"
  def platform_for(%{client: "web"}), do: "web"
  def platform_for(_properties), do: "desktop"

  @doc "PRESENCE_UPDATE dispatch payload; `guild_id` is omitted for friend presence."
  @spec dispatch_payload(String.t(), aggregate(), String.t() | nil) :: map()
  def dispatch_payload(user_id, aggregate, guild_id) do
    payload = %{
      "user" => %{"id" => user_id},
      "status" => aggregate.status,
      "activities" => aggregate.activities,
      "client_status" => aggregate.client_status
    }

    if guild_id, do: Map.put(payload, "guild_id", guild_id), else: payload
  end

  defp client_status(visible) do
    visible
    |> Enum.group_by(& &1.platform)
    |> Map.new(fn {platform, list} -> {platform, Enum.max_by(list, &@priority[&1.status]).status} end)
  end
end
