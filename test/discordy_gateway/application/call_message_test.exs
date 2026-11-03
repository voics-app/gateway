defmodule DiscordyGateway.Application.CallMessageTest do
  @moduledoc "gateway#39: a DM call is mirrored into a call system message (type 3) in messages."

  use DiscordyGateway.Case, async: false

  defp setup_call(channel) do
    for user <- ~w(2 3) do
      Fakes.put(
        {:access, channel, user},
        {:ok,
         %{
           "guild_id" => nil,
           "delivery_mode" => "standard",
           "recipient_ids" => ["2", "3"],
           "channel_type" => 1
         }}
      )
    end

    Fakes.put(:voice_nodes, [voice_node("a")])
    {online("2"), online("3")}
  end

  defp voice(client, channel_id) do
    send_frame(client.session, 4, %{
      "guild_id" => nil,
      "channel_id" => channel_id,
      "self_mute" => false,
      "self_deaf" => false
    })
  end

  defp calls(channel) do
    for call <- Fakes.calls(:messages_client), elem(call, 1) == channel, do: call
  end

  defp wait_for(channel, count, tries \\ 40) do
    cond do
      length(calls(channel)) >= count -> calls(channel)
      tries == 0 -> flunk("expected #{count} messages calls, got #{inspect(calls(channel))}")
      true -> Process.sleep(10) && wait_for(channel, count, tries - 1)
    end
  end

  test "posts the call message when the call starts, updates it on joins and ends it" do
    {alice, bob} = setup_call("cm1")

    voice(alice, "cm1")
    assert [{:create, "cm1", "2", ["2"]}] = wait_for("cm1", 1)

    voice(bob, "cm1")

    assert [_, {:update, "cm1", "call-message-cm1", %{participants: ["2", "3"]}}] =
             wait_for("cm1", 2)

    voice(alice, nil)
    voice(bob, nil)

    assert [_, _, {:update, "cm1", "call-message-cm1", ended}] = wait_for("cm1", 3)
    assert %{participants: ["2", "3"], ended_timestamp: stamp} = ended
    assert {:ok, _datetime, 0} = DateTime.from_iso8601(stamp)
  end

  test "a failing messages service never breaks the call" do
    Fakes.put(:messages_client_failing, true)
    {alice, bob} = setup_call("cm2")

    voice(alice, "cm2")
    assert [{:create, "cm2", "2", ["2"]}] = wait_for("cm2", 1)
    assert %{"channel_id" => "cm2", "user_id" => "2"} = await(bob, "VOICE_STATE_UPDATE")
    assert %{"channel_id" => "cm2"} = await(alice, "VOICE_SERVER_UPDATE")

    voice(alice, nil)
    assert %{"channel_id" => nil} = await(bob, "VOICE_STATE_UPDATE")
    assert length(calls("cm2")) == 1
  end
end
