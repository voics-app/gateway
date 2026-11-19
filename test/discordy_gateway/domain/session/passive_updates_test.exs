defmodule DiscordyGateway.Domain.Session.PassiveUpdatesTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Session.PassiveUpdates

  test "coalesces only changed fields and keeps the latest marker and read version" do
    read = %{"channel_id" => "2", "last_message_id" => "3", "mention_count" => 1, "read_state_version" => "9007199254740994"}

    changes =
      %PassiveUpdates{}
      |> PassiveUpdates.message("1", %{"channel_id" => "2", "id" => "5", "content" => "secret"})
      |> PassiveUpdates.message("1", %{"channel_id" => "2", "id" => "4"})
      |> PassiveUpdates.read_state(read)
      |> PassiveUpdates.read_state(%{read | "read_state_version" => "9007199254740993", "mention_count" => 99})

    assert PassiveUpdates.payload(changes) == %{
             "channels" => [%{"guild_id" => "1", "channel_id" => "2", "last_message_id" => "5"}],
             "read_states" => [read]
           }
    assert PassiveUpdates.payload(%PassiveUpdates{}) == %{}
  end

  test "keeps the latest voice state without member data" do
    voice = %{"guild_id" => "1", "user_id" => "2", "session_id" => "3", "channel_id" => "4", "member" => %{"private" => true}}
    changes = %PassiveUpdates{} |> PassiveUpdates.voice_state(voice) |> PassiveUpdates.voice_state(%{voice | "channel_id" => nil})
    assert PassiveUpdates.payload(changes) == %{"voice_states" => [voice |> Map.put("channel_id", nil) |> Map.delete("member")]}
  end
end
