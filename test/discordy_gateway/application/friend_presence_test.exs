defmodule DiscordyGateway.Application.FriendPresenceTest do
  @moduledoc "A friendship formed after READY hands over presence and tracks later changes."

  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Events.Ingest

  defp ready_for(user_id) do
    {:ok,
     %{
       "user" => %{"id" => user_id},
       "guilds" => [],
       "private_channels" => [],
       "relationships" => [],
       "user_settings" => %{}
     }}
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

  defp befriend(a, b) do
    publish("RELATIONSHIP_ADD", %{"id" => b, "type" => 1, "user" => %{"id" => b}}, [a])
    publish("RELATIONSHIP_ADD", %{"id" => a, "type" => 1, "user" => %{"id" => a}}, [b])
  end

  setup do
    for user <- ~w(7 8), do: Fakes.put({:ready, user}, ready_for(user))
    %{first: online("7"), second: online("8")}
  end

  test "both online users see each other online right after the friend request is accepted", %{
    first: first,
    second: second
  } do
    befriend("7", "8")

    assert await_presence(first, "8", "online") != :none
    assert await_presence(second, "7", "online") != :none
  end

  test "later presence changes reach the new friend", %{first: first, second: second} do
    befriend("7", "8")
    await_presence(second, "7", "online")

    send_frame(first.session, 3, %{"status" => "dnd", "activities" => [], "afk" => false})

    assert await_presence(second, "7", "dnd") != :none
  end

  test "a pending request does not share presence", %{first: first} do
    publish("RELATIONSHIP_ADD", %{"id" => "8", "type" => 4, "user" => %{"id" => "8"}}, ["7"])

    assert await_presence(first, "8", "online", 150) == :none
  end

  test "a removed friend no longer receives presence changes", %{first: first, second: second} do
    befriend("7", "8")
    await_presence(second, "7", "online")
    publish("RELATIONSHIP_REMOVE", %{"id" => "7", "type" => 1}, ["8"])
    publish("RELATIONSHIP_REMOVE", %{"id" => "8", "type" => 1}, ["7"])
    await(second, "RELATIONSHIP_REMOVE")
    await(first, "RELATIONSHIP_REMOVE")

    send_frame(first.session, 3, %{"status" => "dnd", "activities" => [], "afk" => false})

    assert await_presence(second, "7", "dnd", 150) == :none
  end
end
