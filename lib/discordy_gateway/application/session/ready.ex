defmodule DiscordyGateway.Application.Session.Ready do
  @moduledoc "Builds the READY payload from the api `/ready` response (P4) and the live state."

  alias DiscordyGateway.Application.{Ports, Settings}
  alias DiscordyGateway.Application.Presence.Publisher
  alias DiscordyGateway.Application.Session.Identity

  @friend_type 1

  @spec payload(Identity.t(), String.t(), map()) :: map()
  def payload(%Identity{} = identity, session_id, ready) do
    %{
      "v" => 1,
      "user" => ready["user"],
      "session_id" => session_id,
      "resume_gateway_url" => Settings.public_gateway_url(),
      "guilds" => Enum.map(guild_ids(ready), &%{"id" => &1, "unavailable" => true}),
      "private_channels" => List.wrap(ready["private_channels"]),
      "relationships" => List.wrap(ready["relationships"]),
      "user_settings" => ready["user_settings"] || %{},
      "user_guild_settings" => List.wrap(ready["user_guild_settings"]),
      "notes" => ready["notes"] || %{},
      "presences" => Publisher.friend_payloads(friend_ids(ready))
    }
    |> put_application(identity)
    |> put_turn(identity)
  end

  @doc "Whether the user has the `e2ee_calls_default` account setting on (every call is E2EE)."
  @spec e2ee_calls_default?(map()) :: boolean()
  def e2ee_calls_default?(ready),
    do: get_in(ready, ["user_settings", "e2ee_calls_default"]) == true

  @doc "Ids of the guilds in the api response."
  @spec guild_ids(map()) :: [String.t()]
  def guild_ids(ready), do: for(%{"id" => id} <- List.wrap(ready["guilds"]), do: id)

  @doc "Ids of the user's friends, whose presence is delivered directly."
  @spec friend_ids(map()) :: [String.t()]
  def friend_ids(ready) do
    for %{"type" => @friend_type} = relationship <- List.wrap(ready["relationships"]),
        id = relationship["id"] || get_in(relationship, ["user", "id"]),
        is_binary(id),
        do: id
  end

  defp put_application(payload, %Identity{kind: :bot, application_id: id}) when is_binary(id),
    do: Map.put(payload, "application", %{"id" => id})

  defp put_application(payload, _identity), do: payload

  defp put_turn(payload, %Identity{kind: :user, user_id: user_id}) do
    credentials = Ports.turn_credentials().issue(user_id, System.os_time(:second))

    Map.put(payload, "turn", %{
      "urls" => credentials.urls,
      "username" => credentials.username,
      "credential" => credentials.credential,
      "ttl" => credentials.ttl
    })
  end

  defp put_turn(payload, _identity), do: payload
end
