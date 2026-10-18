defmodule DiscordyGateway.Application.SessionTest do
  use DiscordyGateway.Case, async: false

  import Bitwise

  alias DiscordyGateway.Application.Session

  test "sends HELLO with the heartbeat interval" do
    {_session, hello} = connect()
    assert %{"op" => 10, "d" => %{"heartbeat_interval" => 200}} = hello
  end

  test "IDENTIFY yields READY with seq 1, TURN credentials and the session id" do
    {session, _} = connect()
    identify(session, "1")

    assert %{"s" => 1, "d" => ready} = next_dispatch("READY")
    assert ready["user"] == %{"id" => "1"}
    assert ready["resume_gateway_url"] == "ws://localhost:4000/gateway"
    assert ready["turn"]["urls"] == ["turn:turn.test:3478"]
    assert is_binary(ready["session_id"])
  end

  test "authentication failure closes with 4004" do
    {session, _} = connect()
    send_frame(session, 2, %{"token" => "garbage", "intents" => 0})
    assert_receive {:gw_close, 4004}
  end

  test "an unknown opcode closes with 4001, a malformed payload with 4002" do
    {session, _} = connect()
    send_frame(session, 99, nil)
    assert_receive {:gw_close, 4001}

    {session, _} = connect()
    send_frame(session, 2, "nope")
    assert_receive {:gw_close, 4002}
  end

  test "commands before IDENTIFY close with 4003" do
    {session, _} = connect()
    send_frame(session, 3, %{"status" => "online", "activities" => [], "afk" => false})
    assert_receive {:gw_close, 4003}
  end

  test "no IDENTIFY in time closes with 4003" do
    {_session, _} = connect()
    assert_receive {:gw_close, 4003}, 1_000
  end

  test "a second IDENTIFY closes with 4005" do
    {session, _} = connect()
    identify(session, "1")
    next_dispatch("READY")
    identify(session, "1")
    assert_receive {:gw_close, 4005}
  end

  test "bots may not request privileged intents they are not approved for (4014) nor undefined bits (4013)" do
    {session, _} = connect()
    send_frame(session, 2, %{"token" => "Bot limited-bot", "intents" => 1 <<< 15})
    assert_receive {:gw_close, 4014}

    {session, _} = connect()
    send_frame(session, 2, %{"token" => "Bot good-bot", "intents" => 1 <<< 20})
    assert_receive {:gw_close, 4013}
  end

  test "a bot IDENTIFY succeeds with its own intents and READY carries the application" do
    {session, _} = connect()
    send_frame(session, 2, %{"token" => "Bot good-bot", "intents" => 1})
    assert %{"d" => %{"application" => %{"id" => "900"} = ready}} = next_dispatch("READY")
    refute Map.has_key?(ready, "turn")
  end

  test "HEARTBEAT is acknowledged, and a seq in the future closes with 4007" do
    {session, _} = connect()
    send_frame(session, 1, nil)
    assert %{"op" => 11} = next_frame()
    send_frame(session, 1, 50)
    assert_receive {:gw_close, 4007}
  end

  test "a silent connection times out with 4009 once identified" do
    {session, _} = connect()
    identify(session, "1")
    next_dispatch("READY")
    assert_receive {:gw_close, 4009}, 1_000
    assert Process.alive?(session)
  end

  test "exceeding 120 frames per window closes with 4008" do
    {session, _} = connect()
    for _ <- 1..121, do: send_frame(session, 1, nil)
    assert_receive {:gw_close, 4008}, 1_000
  end

  test "dispatches are numbered, buffered, and RESUME replays what the client missed" do
    {session, _} = connect()
    identify(session, "1")
    %{"d" => %{"session_id" => session_id}} = next_dispatch("READY")
    flush_frames()

    send(session, {:dispatch, "TYPING_START", ~s({"a":1})})
    assert %{"s" => 2, "t" => "TYPING_START"} = next_dispatch("TYPING_START")
    send(session, {:dispatch, "TYPING_START", ~s({"a":2})})
    assert %{"s" => 3} = next_dispatch("TYPING_START")

    # the connection drops; a new transport resumes from seq 1
    old = spawn(fn -> :ok end)
    :sys.replace_state(session, fn state -> %{state | transport: old} end)
    {:ok, stub} = Session.start(self())
    assert %{"op" => 10} = next_frame()
    send_frame(stub, 6, %{"token" => "user-1", "session_id" => session_id, "seq" => 1})

    assert %{"s" => 2} = next_dispatch("TYPING_START")
    assert %{"s" => 3} = next_dispatch("TYPING_START")
    assert %{"s" => 4} = next_dispatch("RESUMED")
  end

  test "RESUME for an unknown session answers INVALID_SESSION(false)" do
    {stub, _} = connect()
    send_frame(stub, 6, %{"token" => "user-1", "session_id" => "nope", "seq" => 1})
    assert %{"op" => 9, "d" => false} = next_frame()
  end

  test "RESUME with a seq ahead of the session closes with 4007" do
    {session, _} = connect()
    identify(session, "1")
    %{"d" => %{"session_id" => session_id}} = next_dispatch("READY")

    {:ok, stub} = Session.start(self())
    assert %{"op" => 10} = next_frame()
    send_frame(stub, 6, %{"token" => "user-1", "session_id" => session_id, "seq" => 9})
    assert_receive {:gw_close, 4007}
  end

  test "a detached session expires after the resume window and cleans up its buffer" do
    transport = spawn(fn -> Process.sleep(:infinity) end)
    {:ok, session} = Session.start(transport)
    Session.frame(session, %{"op" => 2, "d" => %{"token" => "user-1", "intents" => 0}})
    Process.sleep(50)
    ref = Process.monitor(session)
    Process.exit(transport, :kill)
    assert_receive {:DOWN, ^ref, :process, ^session, :normal}, 1_000
    assert Fakes.get({:resume, "x"}) == nil
  end

  test "user-targeted events honour direct intents for bots" do
    {session, _} = connect()
    send_frame(session, 2, %{"token" => "Bot good-bot", "intents" => 1})
    next_dispatch("READY")
    flush_frames()

    envelope = %DiscordyGateway.Domain.Events.EventEnvelope{
      id: "e1",
      type: "MESSAGE_CREATE",
      data: %{"id" => "m1"},
      target_user_ids: ["900"]
    }

    send(session, {:user_event, envelope})
    assert next_dispatch("MESSAGE_CREATE", 100) == :none

    send(session, {:user_event, %{envelope | type: "USER_UPDATE"}})
    assert %{"t" => "USER_UPDATE"} = next_dispatch("USER_UPDATE")
  end
end
