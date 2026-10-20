defmodule DiscordyGateway.Application.NotificationSyncTest do
  @moduledoc "api#28 / api#51: notification settings and notes in READY and their per-user dispatch."

  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Events.Ingest

  @guild_settings %{
    "guild_id" => "g1",
    "message_notifications" => 1,
    "suppress_everyone" => true,
    "suppress_roles" => false,
    "muted" => true,
    "mute_config" => %{"end_time" => nil},
    "hide_muted_channels" => false,
    "channel_overrides" => [
      %{
        "channel_id" => "10",
        "message_notifications" => 2,
        "muted" => false,
        "mute_config" => nil
      }
    ]
  }

  defp ready_for(user_id, extra) do
    {:ok,
     Map.merge(
       %{
         "user" => %{"id" => user_id},
         "guilds" => [],
         "private_channels" => [],
         "relationships" => [],
         "user_settings" => %{}
       },
       extra
     )}
  end

  defp publish(type, data, targets) do
    envelope = %{
      "id" => unique_id(),
      "type" => type,
      "v" => 1,
      "data" => data,
      "target_user_ids" => targets,
      "occurred_at" => "2026-10-20T00:00:00Z"
    }

    Ingest.handle(:user, Jason.encode!(envelope))
  end

  test "READY carries the user's guild notification settings and notes" do
    Fakes.put(
      {:ready, "7"},
      ready_for("7", %{
        "user_guild_settings" => [@guild_settings],
        "notes" => %{"8" => "met at the conference"}
      })
    )

    client = client("7")
    identify(client.session, "7")

    assert %{"user_guild_settings" => [settings], "notes" => notes} = await(client, "READY")
    assert settings == @guild_settings
    assert notes == %{"8" => "met at the conference"}
  end

  test "READY defaults to no settings and no notes when the api sends none" do
    Fakes.put({:ready, "7"}, ready_for("7", %{}))

    assert %{"user_guild_settings" => [], "notes" => %{}} = await(client_ready("7"), "READY")
  end

  defp client_ready(user_id) do
    client = client(user_id)
    identify(client.session, user_id)
    client
  end

  test "USER_GUILD_SETTINGS_UPDATE reaches every session of the user and nobody else" do
    for user <- ~w(7 8), do: Fakes.put({:ready, user}, ready_for(user, %{}))
    first = online("7")
    second = online("7")
    other = online("8")

    publish("USER_GUILD_SETTINGS_UPDATE", @guild_settings, ["7"])

    assert await(first, "USER_GUILD_SETTINGS_UPDATE") == @guild_settings
    assert await(second, "USER_GUILD_SETTINGS_UPDATE") == @guild_settings
    assert await(other, "USER_GUILD_SETTINGS_UPDATE", 100) == :none
  end

  test "USER_NOTE_UPDATE reaches only the author's sessions" do
    for user <- ~w(7 8), do: Fakes.put({:ready, user}, ready_for(user, %{}))
    author = online("7")
    author_phone = online("7")
    noted = online("8")

    publish("USER_NOTE_UPDATE", %{"id" => "8", "note" => "hello"}, ["7"])

    assert %{"id" => "8", "note" => "hello"} = await(author, "USER_NOTE_UPDATE")
    assert %{"id" => "8", "note" => "hello"} = await(author_phone, "USER_NOTE_UPDATE")
    assert await(noted, "USER_NOTE_UPDATE", 100) == :none
  end
end
