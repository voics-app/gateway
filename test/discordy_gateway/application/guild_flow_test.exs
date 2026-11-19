defmodule DiscordyGateway.Application.GuildFlowTest do
  use DiscordyGateway.Case, async: false

  import Bitwise

  alias DiscordyGateway.Application.Events.Ingest
  alias DiscordyGateway.Application.Ports
  alias DiscordyGateway.Fixtures

  defp guild_with_bot(gid) do
    payload = Fixtures.guild_payload(gid)
    bot = %{"user" => %{"id" => "900", "username" => "bot", "bot" => true}, "roles" => []}
    Map.update!(payload, "members", &(&1 ++ [bot]))
  end

  defp ready_for(user_id, guilds) do
    {:ok,
     %{
       "user" => %{"id" => user_id},
       "guilds" => guilds,
       "private_channels" => [],
       "relationships" => [],
       "user_settings" => %{}
     }}
  end

  defp setup_guild(gid, users \\ ~w(1 2 3 900)) do
    payload = guild_with_bot(gid)
    Fakes.put({:guild, gid}, {:ok, payload})
    for user <- users, do: Fakes.put({:ready, user}, ready_for(user, [payload]))
    payload
  end

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

  defp online_in(gid, user) do
    client = online(user)
    assert %{"id" => ^gid} = await(client, "GUILD_CREATE")
    client
  end

  defp bot_online(intents) do
    client = client("bot")
    send_frame(client.session, 2, %{"token" => "Bot good-bot", "intents" => intents})
    await(client, "READY")
    client
  end

  describe "GUILD_CREATE" do
    test "hides private threads even when their parent is visible" do
      payload =
        guild_with_bot("private-thread")
        |> Map.put("threads", [
          %{"id" => "21", "type" => 12, "parent_id" => "10", "owner_id" => "2"}
        ])

      Fakes.put({:guild, "private-thread"}, {:ok, payload})
      for user <- ~w(1 2 3), do: Fakes.put({:ready, user}, ready_for(user, [payload]))

      assert %{"threads" => outsider_threads} = await(online("3"), "GUILD_CREATE")
      refute Enum.any?(outsider_threads, &(&1["id"] == "21"))
      assert %{"threads" => creator_threads} = await(online("2"), "GUILD_CREATE")
      assert Enum.any?(creator_threads, &(&1["id"] == "21"))
      assert %{"threads" => owner_threads} = await(online("1"), "GUILD_CREATE")
      assert Enum.any?(owner_threads, &(&1["id"] == "21"))
    end

    test "hides channels the member cannot view" do
      setup_guild("g1")
      outsider = online("3")
      assert %{"channels" => channels} = await(outsider, "GUILD_CREATE")
      assert Enum.sort(Enum.map(channels, & &1["id"])) == ["10", "12"]

      staff = online("2")
      assert %{"channels" => channels, "threads" => threads} = await(staff, "GUILD_CREATE")
      assert "11" in Enum.map(channels, & &1["id"])
      assert "13" in Enum.map(threads, & &1["id"])
    end

    test "is loaded lazily from the api when READY carries only the guild id" do
      payload = Fixtures.guild_payload("g2")
      Fakes.put({:guild, "g2"}, {:ok, payload})
      Fakes.put({:ready, "2"}, ready_for("2", [%{"id" => "g2"}]))

      assert %{"id" => "g2", "member_count" => 3} = await(online("2"), "GUILD_CREATE")
    end

    test "READY lists guilds as unavailable until GUILD_CREATE" do
      setup_guild("g3")
      client = client("2")
      identify(client.session, "2")
      assert %{"guilds" => [%{"id" => "g3", "unavailable" => true}]} = await(client, "READY")
    end
  end

  describe "event fan-out" do
    test "membership changes grant and revoke private thread visibility" do
      gid = "thread-members"

      payload =
        guild_with_bot(gid)
        |> Map.put("threads", [
          %{"id" => "21", "type" => 12, "parent_id" => "10", "owner_id" => "2"}
        ])
        |> Map.put("thread_members", %{"21" => %{"version" => 1, "user_ids" => ["2"]}})

      Fakes.put({:guild, gid}, {:ok, payload})
      for user <- ~w(2 3), do: Fakes.put({:ready, user}, ready_for(user, [payload]))
      owner = online_in(gid, "2")
      member = online_in(gid, "3")

      publish(
        :guild,
        "THREAD_MEMBERS_UPDATE",
        %{
          "id" => "21",
          "guild_id" => gid,
          "added_members" => [%{"user_id" => "3"}],
          "removed_member_ids" => [],
          "_membership_version" => 2,
          "_member_ids" => ["2", "3"]
        },
        %{"guild_id" => gid, "channel_id" => "21"}
      )

      assert %{"id" => "21"} = await(member, "THREAD_CREATE")
      assert %{"id" => "21"} = public_update = await(owner, "THREAD_MEMBERS_UPDATE")
      refute Map.has_key?(public_update, "_member_ids")
      refute Map.has_key?(public_update, "_membership_version")

      publish(
        :guild,
        "THREAD_MEMBERS_UPDATE",
        %{
          "id" => "21",
          "guild_id" => gid,
          "added_members" => [],
          "removed_member_ids" => ["3"],
          "_membership_version" => 3,
          "_member_ids" => ["2"]
        },
        %{"guild_id" => gid, "channel_id" => "21"}
      )

      assert %{"id" => "21"} = await(member, "THREAD_DELETE")
    end

    test "delivers channel events only to members who can view the channel" do
      setup_guild("g10")
      staff = online_in("g10", "2")
      outsider = online_in("g10", "3")

      publish(
        :message,
        "MESSAGE_CREATE",
        %{"id" => "m1", "channel_id" => "11", "content" => "secret"},
        %{"guild_id" => "g10", "channel_id" => "11"}
      )

      assert %{"id" => "m1"} = await(staff, "MESSAGE_CREATE")
      assert await(outsider, "MESSAGE_CREATE", 100) == :none

      publish(:message, "MESSAGE_CREATE", %{"id" => "m2", "channel_id" => "10"}, %{
        "guild_id" => "g10",
        "channel_id" => "10"
      })

      assert %{"id" => "m2"} = await(outsider, "MESSAGE_CREATE")
    end

    test "is idempotent on the envelope id" do
      setup_guild("g11")
      client = online_in("g11", "2")
      fields = %{"id" => "same-id", "guild_id" => "g11", "channel_id" => "10"}
      publish(:message, "MESSAGE_CREATE", %{"id" => "m1", "channel_id" => "10"}, fields)
      publish(:message, "MESSAGE_CREATE", %{"id" => "m1", "channel_id" => "10"}, fields)

      assert %{"id" => "m1"} = await(client, "MESSAGE_CREATE")
      assert await(client, "MESSAGE_CREATE", 100) == :none
    end

    test "strips content for bots without MESSAGE_CONTENT, keeps it when they are mentioned" do
      setup_guild("g12")
      plain = bot_online(1 <<< 9)
      assert await(plain, "GUILD_CREATE")
      mention = %{"id" => "m1", "channel_id" => "10", "content" => "hi", "mentions" => []}
      publish(:message, "MESSAGE_CREATE", mention, %{"guild_id" => "g12", "channel_id" => "10"})
      assert %{"content" => "", "id" => "m1"} = await(plain, "MESSAGE_CREATE")

      publish(:message, "MESSAGE_CREATE", %{mention | "mentions" => [%{"id" => "900"}]}, %{
        "guild_id" => "g12",
        "channel_id" => "10"
      })

      assert %{"content" => "hi"} = await(plain, "MESSAGE_CREATE")
    end

    test "respects intents: a bot without GUILD_MESSAGES gets no message events" do
      setup_guild("g13")
      bot = bot_online(1)
      assert await(bot, "GUILD_CREATE")

      publish(:message, "MESSAGE_CREATE", %{"id" => "m1", "channel_id" => "10"}, %{
        "guild_id" => "g13",
        "channel_id" => "10"
      })

      assert await(bot, "MESSAGE_CREATE", 100) == :none
    end

    test "keeps the permission cache current from guild events" do
      setup_guild("g14")
      outsider = online_in("g14", "3")

      publish(
        :guild,
        "GUILD_MEMBER_UPDATE",
        %{"guild_id" => "g14", "user" => %{"id" => "3"}, "roles" => ["300"]},
        %{"guild_id" => "g14"}
      )

      assert %{"roles" => ["300"]} = await(outsider, "GUILD_MEMBER_UPDATE")

      publish(:message, "MESSAGE_CREATE", %{"id" => "m1", "channel_id" => "11"}, %{
        "guild_id" => "g14",
        "channel_id" => "11"
      })

      assert %{"id" => "m1"} = await(outsider, "MESSAGE_CREATE")
    end

    test "a removed member is told GUILD_DELETE and stops receiving events" do
      setup_guild("g15")
      member = online_in("g15", "3")
      other = online_in("g15", "2")

      publish(:guild, "GUILD_MEMBER_REMOVE", %{"guild_id" => "g15", "user" => %{"id" => "3"}}, %{
        "guild_id" => "g15"
      })

      assert %{"id" => "g15"} = await(member, "GUILD_DELETE")
      assert %{"user" => %{"id" => "3"}} = await(other, "GUILD_MEMBER_REMOVE")

      publish(:message, "MESSAGE_CREATE", %{"id" => "m1", "channel_id" => "10"}, %{
        "guild_id" => "g15",
        "channel_id" => "10"
      })

      assert await(other, "MESSAGE_CREATE")
      assert await(member, "MESSAGE_CREATE", 100) == :none
    end

    test "GUILD_MEMBER_ADD makes the new member's online sessions join the guild" do
      payload = setup_guild("g16", ~w(1 2))
      Fakes.put({:guild, "g16"}, {:ok, payload})
      Fakes.put({:ready, "5"}, ready_for("5", []))
      newcomer = online("5")
      existing = online_in("g16", "2")

      member = %{
        "guild_id" => "g16",
        "user" => %{"id" => "5", "username" => "new"},
        "roles" => []
      }

      publish(:guild, "GUILD_MEMBER_ADD", member, %{"guild_id" => "g16"})

      assert %{"user" => %{"id" => "5"}} = await(existing, "GUILD_MEMBER_ADD")
      assert %{"id" => "g16"} = await(newcomer, "GUILD_CREATE")
    end

    test "user-targeted events reach every session of the user" do
      Fakes.put({:ready, "7"}, ready_for("7", []))
      first = online("7")
      second = online("7")

      publish(:user, "USER_UPDATE", %{"id" => "7", "username" => "renamed"}, %{
        "target_user_ids" => ["7"]
      })

      assert %{"username" => "renamed"} = await(first, "USER_UPDATE")
      assert %{"username" => "renamed"} = await(second, "USER_UPDATE")
    end

    test "malformed records are dropped without crashing" do
      assert :ok = Ingest.handle(:message, "not json")
      assert :ok = Ingest.handle(:message, ~s({"id":1}))
    end

    test "interaction events go to the bot's sessions" do
      setup_guild("g17")
      bot = bot_online(1)
      publish(:interaction, "INTERACTION_CREATE", %{"id" => "i1", "application_id" => "900"}, %{})
      assert %{"id" => "i1"} = await(bot, "INTERACTION_CREATE")
    end

    test "GUILD_DELETE tells subscribers and stops the guild process" do
      setup_guild("g18")
      client = online_in("g18", "2")
      publish(:guild, "GUILD_DELETE", %{"id" => "g18"}, %{"guild_id" => "g18"})
      assert await(client, "GUILD_DELETE")
      Process.sleep(50)
      assert :error = Ports.server_directory().lookup({:guild, "g18"})
    end
  end

  describe "presence" do
    test "aggregates across sessions and fans out to guild members and friends" do
      payload = setup_guild("g20", ~w(2 3))
      friend_ready = ready_for("8", [])
      Fakes.put({:ready, "8"}, friend_ready)

      {:ok, ready_2} = ready_for("2", [payload])

      Fakes.put(
        {:ready, "2"},
        {:ok, Map.put(ready_2, "relationships", [%{"id" => "8", "type" => 1}])}
      )

      {:ok, ready_8} = friend_ready

      Fakes.put(
        {:ready, "8"},
        {:ok, Map.put(ready_8, "relationships", [%{"id" => "2", "type" => 1}])}
      )

      watcher = online_in("g20", "3")
      friend = online("8")
      user = online_in("g20", "2")
      assert await_presence(watcher, "2", "online")
      assert await_presence(friend, "2", "online")

      send_frame(user.session, 3, %{"status" => "dnd", "activities" => [], "afk" => false})
      assert %{"guild_id" => "g20"} = await_presence(watcher, "2", "dnd")
      assert await_presence(friend, "2", "dnd")

      send_frame(user.session, 3, %{"status" => "invisible", "activities" => [], "afk" => false})
      assert await_presence(watcher, "2", "offline")
    end

    test "ending the last session announces offline" do
      setup_guild("g21", ~w(2 3))
      watcher = online_in("g21", "3")
      user = online_in("g21", "2")
      assert await_presence(watcher, "2", "online")

      GenServer.stop(user.session)
      assert await_presence(watcher, "2", "offline")
    end
  end

  describe "@here notifies online members only (gateway#39)" do
    setup do
      setup_guild("g40", ~w(2 3))
      online_member = online_in("g40", "2")
      hidden_member = online_in("g40", "3")

      send_frame(hidden_member.session, 3, %{
        "status" => "invisible",
        "activities" => [],
        "afk" => false
      })

      assert await_presence(online_member, "3", "offline")
      %{online: online_member, hidden: hidden_member}
    end

    defp broadcast(flags) do
      data = Map.merge(%{"id" => "m1", "channel_id" => "10", "content" => "hi"}, flags)
      publish(:message, "MESSAGE_CREATE", data, %{"guild_id" => "g40", "channel_id" => "10"})
    end

    test "offline members still get the message, without the broadcast flags", ctx do
      broadcast(%{
        "mention_everyone" => true,
        "mention_here" => true,
        "mentions" => [%{"id" => "3"}]
      })

      assert %{"mention_everyone" => true, "mention_here" => true} =
               await(ctx.online, "MESSAGE_CREATE")

      quiet = await(ctx.hidden, "MESSAGE_CREATE")
      assert %{"id" => "m1", "mention_everyone" => false, "mentions" => [%{"id" => "3"}]} = quiet
      refute Map.has_key?(quiet, "mention_here")
    end

    test "@everyone still reaches every member", ctx do
      broadcast(%{"mention_everyone" => true})

      assert %{"mention_everyone" => true} = await(ctx.online, "MESSAGE_CREATE")
      assert %{"mention_everyone" => true} = await(ctx.hidden, "MESSAGE_CREATE")
    end
  end

  describe "REQUEST_GUILD_MEMBERS" do
    test "answers with chunks for a query and for explicit ids" do
      setup_guild("g30")
      client = online_in("g30", "2")

      send_frame(client.session, 8, %{
        "guild_id" => "g30",
        "query" => "al",
        "limit" => 10,
        "nonce" => "n1"
      })

      assert %{
               "members" => [%{"user" => %{"id" => "2"}}],
               "chunk_index" => 0,
               "chunk_count" => 1,
               "nonce" => "n1"
             } =
               await(client, "GUILD_MEMBERS_CHUNK")

      send_frame(client.session, 8, %{
        "guild_id" => "g30",
        "user_ids" => ["3", "404"],
        "limit" => 0
      })

      assert %{"members" => [%{"user" => %{"id" => "3"}}], "not_found" => ["404"]} =
               await(client, "GUILD_MEMBERS_CHUNK")
    end
  end
end
