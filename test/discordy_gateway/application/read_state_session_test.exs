defmodule DiscordyGateway.Application.ReadStateSessionTest do
  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Ports
  alias DiscordyGateway.Domain.Events.EventEnvelope

  test "READY hydrates certified state including an empty account" do
    Fakes.put({:read_states, "1"}, {:ok, %{entries: [], versioned: true}})
    {session, _} = connect()
    identify(session, "1")

    assert %{"d" => %{"read_state" => [], "read_state_versioned" => true}} =
             next_dispatch("READY")
  end

  test "failed hydration is retryable and never certifies empty READY" do
    Fakes.put({:read_states, "1"}, {:error, :unavailable})
    {session, _} = connect()
    identify(session, "1")
    assert_receive {:gw_close, 4000}
    assert :none = next_dispatch("READY", 50)
  end

  test "subscription precedes fetch and an in-flight update is delivered after READY" do
    test = self()
    snapshot = entry("1", 1)

    update =
      snapshot
      |> Map.delete("id")
      |> Map.merge(%{"channel_id" => "200", "read_state_version" => "2", "mention_count" => 2})

    Fakes.put({:read_states, "1"}, {:ok, %{entries: [snapshot], versioned: true}})

    Fakes.put(:read_states_hook, fn user_id ->
      send(
        test,
        {:subscribed_before_fetch, self() in Ports.session_directory().sessions_for_user(user_id)}
      )

      send(
        self(),
        {:user_event,
         %EventEnvelope{
           id: "read-change",
           type: "READ_STATE_UPDATE",
           target_user_ids: [user_id],
           data: update
         }}
      )
    end)

    {session, _} = connect()
    identify(session, "1")
    assert_receive {:subscribed_before_fetch, true}

    assert %{"t" => "READY", "d" => %{"read_state" => [%{"read_state_version" => "1"}]}} =
             next_frame()

    assert %{"t" => "READ_STATE_UPDATE", "d" => ^update} = next_frame()
  end

  test "legacy hydration keeps counts without certifying authority" do
    legacy = Map.delete(entry("0", 7), "read_state_version")
    Fakes.put({:read_states, "1"}, {:ok, %{entries: [legacy], versioned: false}})
    {session, _} = connect()
    identify(session, "1")
    assert %{"d" => ready} = next_dispatch("READY")
    assert ready["read_state"] == [legacy]
    refute Map.has_key?(ready, "read_state_versioned")
  end

  for {name, result} <- [
        {"failed snapshot", {:error, :unavailable}},
        {"lost certification", {:ok, %{entries: [], versioned: false}}}
      ] do
    test "resume invalidates the old session after #{name}" do
      Fakes.put({:read_states, "1"}, {:ok, %{entries: [], versioned: true}})
      {session, _} = connect()
      identify(session, "1")
      %{"d" => %{"session_id" => id}} = next_dispatch("READY")
      Fakes.put({:read_states, "1"}, unquote(Macro.escape(result)))
      {stub, _} = connect()
      send_frame(stub, 6, %{"token" => "user-1", "session_id" => id, "seq" => 1})
      assert %{"op" => 9, "d" => false} = next_frame()
      refute Process.alive?(session)
    end
  end

  test "resume fetches missed publication and syncs after replay before RESUMED" do
    old = entry("2", 1)
    Fakes.put({:read_states, "1"}, {:ok, %{entries: [old], versioned: true}})
    {session, _} = connect()
    identify(session, "1")
    %{"d" => %{"session_id" => session_id}} = next_dispatch("READY")
    flush_frames()

    send(
      session,
      {:dispatch, "READ_STATE_UPDATE", Jason.encode!(Map.put(entry("4", 3), "channel_id", "200"))}
    )

    assert %{"s" => 2} = next_dispatch("READ_STATE_UPDATE")
    Fakes.put({:read_states, "1"}, {:ok, %{entries: [entry("3", 2)], versioned: true}})
    stale_transport = spawn(fn -> :ok end)
    :sys.replace_state(session, &%{&1 | transport: stale_transport})
    {stub, _} = connect()
    send_frame(stub, 6, %{"token" => "user-1", "session_id" => session_id, "seq" => 1})
    assert %{"t" => "READ_STATE_UPDATE", "d" => %{"read_state_version" => "4"}} = next_frame()

    assert %{"t" => "READ_STATE_SYNC", "d" => %{"read_state" => [%{"read_state_version" => "3"}]}} =
             next_frame()

    assert %{"t" => "RESUMED"} = next_frame()
  end

  defp entry(version, count) do
    %{
      "id" => "200",
      "last_message_id" => "500",
      "mention_count" => count,
      "read_state_version" => version
    }
  end
end
