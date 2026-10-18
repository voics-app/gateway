defmodule DiscordyGateway.Application.GuildChannelEventsTest do
  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Events.Ingest
  alias DiscordyGateway.Fixtures

  @new_channel %{
    "id" => "20",
    "type" => 2,
    "name" => "fresh-voice",
    "permission_overwrites" => [],
    "guild_id" => "n1"
  }

  defp publish(topic, type, data, fields) do
    envelope =
      Map.merge(
        %{
          "id" => unique_id(),
          "type" => type,
          "v" => 1,
          "data" => data,
          "occurred_at" => "2026-10-18T00:00:00Z"
        },
        fields
      )

    Ingest.handle(topic, Jason.encode!(envelope))
  end

  defp with_channel(payload, channel), do: Map.update!(payload, "channels", &(&1 ++ [channel]))

  defp ready_without_guilds(user_id) do
    Fakes.put(
      {:ready, user_id},
      {:ok,
       %{
         "user" => %{"id" => user_id},
         "guilds" => [],
         "private_channels" => [],
         "relationships" => []
       }}
    )
  end

  defp join(client, gid, channel) do
    send_frame(client.session, 4, %{
      "guild_id" => gid,
      "channel_id" => channel,
      "self_mute" => false,
      "self_deaf" => false
    })
  end

  defp announce_guild(payload) do
    publish(:guild, "GUILD_CREATE", payload, %{
      "guild_id" => payload["id"],
      "target_user_ids" => ["1"]
    })
  end

  defp announce_channel(gid, channel) do
    publish(:guild, "CHANNEL_CREATE", channel, %{"guild_id" => gid, "channel_id" => channel["id"]})
  end

  test "a channel created right after the guild, before the guild process exists, is joinable" do
    ready_without_guilds("1")
    Fakes.put(:voice_nodes, [voice_node("n", region: "eu-west")])
    payload = Fixtures.guild_payload("n0")
    Fakes.put({:guild, "n0"}, {:ok, with_channel(payload, @new_channel)})
    owner = online("1")

    announce_guild(payload)
    announce_channel("n0", @new_channel)

    assert %{"id" => "n0", "channels" => channels} = await(owner, "GUILD_CREATE")
    assert "20" in Enum.map(channels, & &1["id"])
    join(owner, "n0", "20")
    assert %{"channel_id" => "20", "user_id" => "1"} = await(owner, "VOICE_STATE_UPDATE")
    assert %{"channel_id" => "20", "guild_id" => "n0"} = await(owner, "VOICE_SERVER_UPDATE")
  end

  test "GUILD_CREATE is dispatched once for a newly created guild" do
    ready_without_guilds("1")
    payload = Fixtures.guild_payload("n2")
    Fakes.put({:guild, "n2"}, {:ok, payload})
    owner = online("1")

    announce_guild(payload)

    assert %{"id" => "n2"} = await(owner, "GUILD_CREATE")
    assert await(owner, "GUILD_CREATE", 200) == :none
  end

  test "channel create, update and delete update the running guild and reach viewers" do
    ready_without_guilds("1")
    Fakes.put(:voice_nodes, [voice_node("n", region: "eu-west")])
    payload = Fixtures.guild_payload("n1")
    Fakes.put({:guild, "n1"}, {:ok, payload})
    owner = online("1")
    announce_guild(payload)
    assert %{"id" => "n1"} = await(owner, "GUILD_CREATE")

    announce_channel("n1", @new_channel)
    assert %{"id" => "20"} = await(owner, "CHANNEL_CREATE")

    renamed = %{@new_channel | "name" => "renamed"}
    publish(:guild, "CHANNEL_UPDATE", renamed, %{"guild_id" => "n1", "channel_id" => "20"})
    assert %{"name" => "renamed"} = await(owner, "CHANNEL_UPDATE")

    join(owner, "n1", "20")
    assert %{"channel_id" => "20"} = await(owner, "VOICE_STATE_UPDATE")
    assert %{"channel_id" => "20"} = await(owner, "VOICE_SERVER_UPDATE")

    publish(:guild, "CHANNEL_DELETE", renamed, %{"guild_id" => "n1", "channel_id" => "20"})
    assert %{"id" => "20"} = await(owner, "CHANNEL_DELETE")

    join(owner, "n1", "20")
    assert %{"channel_id" => nil} = await(owner, "VOICE_STATE_UPDATE", 300)
  end

  test "voice state updates carry the full guild member" do
    Fakes.put(:voice_nodes, [voice_node("n", region: "us-east")])
    Fixtures.guild_payload("v9") |> then(&Fakes.put({:guild, "v9"}, {:ok, &1}))

    for user <- ~w(2 3) do
      Fakes.put(
        {:ready, user},
        {:ok,
         %{
           "user" => %{"id" => user},
           "guilds" => [%{"id" => "v9"}],
           "private_channels" => [],
           "relationships" => []
         }}
      )
    end

    alice = online("2")
    bob = online("3")
    assert %{"id" => "v9"} = await(alice, "GUILD_CREATE")
    assert %{"id" => "v9"} = await(bob, "GUILD_CREATE")

    join(alice, "v9", "12")

    assert %{"member" => %{"nick" => "Ally", "roles" => ["300"], "user" => %{"id" => "2"} = user}} =
             await_voice_state(bob, "v9")

    assert user["username"] == "alice"
  end

  defp await_voice_state(client, guild_id) do
    case await(client, "VOICE_STATE_UPDATE") do
      %{"guild_id" => ^guild_id} = payload -> payload
      :none -> :none
      _other_scope -> await_voice_state(client, guild_id)
    end
  end
end
