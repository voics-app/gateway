defmodule DiscordyGateway.Application.MobilePresenceTest do
  use DiscordyGateway.Case, async: false

  defp mobile(status) do
    {session, _hello} = connect()

    identify(session, "1", %{
      "properties" => %{"os" => "ios", "client" => "discordy-mobile", "version" => "1"},
      "presence" => %{"status" => status, "activities" => [], "afk" => false}
    })

    next_dispatch("READY")
    :sys.get_state(session)
    on_exit(fn -> if Process.alive?(session), do: GenServer.stop(session) end)
    session
  end

  defp detach(session) do
    state = :sys.get_state(session)
    send(session, {:DOWN, state.transport_ref, :process, self(), :normal})
    :sys.get_state(session)
  end

  test "a minimized phone keeps its idle presence after the ordinary resume window" do
    session = mobile("online")

    send_frame(session, 3, %{
      "status" => "idle",
      "activities" => [],
      "afk" => true,
      "background" => true
    })

    state = detach(session)
    ref = Process.monitor(session)
    send(session, {:resume_expired, state.detach_token})

    refute_receive {:DOWN, ^ref, :process, ^session, _}, 50
    assert %{status: "idle"} = Fakes.get({:aggregate, "1"})
  end

  test "revoking the login clears a detached mobile presence immediately" do
    session = mobile("online")
    send_frame(session, 3, %{"status" => "online", "background" => true})
    detach(session)
    ref = Process.monitor(session)

    send(
      session,
      {:user_event,
       %DiscordyGateway.Domain.Events.EventEnvelope{
         id: "logout",
         type: "SESSION_REVOKE",
         data: %{"session_ids" => ["auth-1"]},
         target_user_ids: ["1"]
       }}
    )

    assert_receive {:DOWN, ^ref, :process, ^session, :normal}
    assert %{status: "offline"} = Fakes.get({:aggregate, "1"})
  end

  test "background expiry removes the phone without taking the desktop offline" do
    phone = mobile("online")
    send_frame(phone, 3, %{"status" => "dnd", "background" => true})
    state = detach(phone)
    {desktop, _hello} = connect()
    identify(desktop, "1", %{"device_id" => "desktop-1"})
    next_dispatch("READY")
    :sys.get_state(desktop)
    assert %{status: "dnd"} = Fakes.get({:aggregate, "1"})
    ref = Process.monitor(phone)
    send(phone, {:background_presence_expired, state.background_token})
    assert_receive {:DOWN, ^ref, :process, ^phone, :normal}

    assert %{status: "online", client_status: %{"desktop" => "online"}} =
             Fakes.get({:aggregate, "1"})

    GenServer.stop(desktop)
  end

  test "explicit online, idle, DND and invisible keep their meaning across suspension" do
    for status <- ~w(online idle dnd invisible) do
      session = mobile("online")
      send_frame(session, 3, %{"status" => status, "background" => true})
      state = detach(session)
      send(session, {:resume_expired, state.detach_token})
      :sys.get_state(session)
      expected = if status == "invisible", do: "offline", else: status
      assert %{status: ^expected} = Fakes.get({:aggregate, "1"})
      GenServer.stop(session)
    end
  end

  test "RESUME clears the old background lease and its expiry cannot end the foreground session" do
    session = mobile("online")
    send_frame(session, 3, %{"status" => "idle", "afk" => true, "background" => true})
    state = detach(session)
    {stub, _hello} = connect()
    send_frame(stub, 6, %{"token" => "user-1", "session_id" => state.id, "seq" => state.seq})
    assert %{"t" => "RESUMED"} = next_dispatch("RESUMED")
    send(session, {:background_presence_expired, state.background_token})
    assert %{background_token: nil, transport: transport} = :sys.get_state(session)
    assert transport == self()
    send_frame(stub, 3, %{"status" => "online", "background" => false})
    :sys.get_state(stub)
    :sys.get_state(session)
    assert %{status: "online"} = Fakes.get({:aggregate, "1"})
    GenServer.stop(stub)
  end

  test "a fresh IDENTIFY replaces the previous background entry for the same mobile login" do
    previous = mobile("online")
    send_frame(previous, 3, %{"status" => "online", "background" => true})
    detach(previous)
    ref = Process.monitor(previous)
    replacement = mobile("invisible")
    assert_receive {:DOWN, ^ref, :process, ^previous, :normal}
    assert map_size(Fakes.get({:presence, "1"})) == 1
    assert %{status: "offline"} = Fakes.get({:aggregate, "1"})
    assert Process.alive?(replacement)
  end

  test "a foreground declaration cancels retention and malformed background values are rejected" do
    session = mobile("online")
    send_frame(session, 3, %{"status" => "idle", "background" => true})
    send_frame(session, 3, %{"status" => "online", "background" => false})
    state = detach(session)
    ref = Process.monitor(session)
    send(session, {:resume_expired, state.detach_token})
    assert_receive {:DOWN, ^ref, :process, ^session, :normal}
    assert %{status: "offline"} = Fakes.get({:aggregate, "1"})

    invalid = mobile("online")
    send_frame(invalid, 3, %{"status" => "online", "background" => "true"})
    assert_receive {:gw_close, 4002}
  end

  test "desktop and bot sessions never gain a mobile background lease" do
    for extra <- [
          %{},
          %{
            "token" => "Bot good-bot",
            "properties" => %{"os" => "ios", "client" => "bot", "version" => "1"}
          }
        ] do
      {session, _hello} = connect()
      identify(session, "1", extra)
      next_dispatch("READY")
      send_frame(session, 3, %{"status" => "online", "background" => true})
      state = detach(session)
      assert state.background_token == nil
      ref = Process.monitor(session)
      send(session, {:resume_expired, state.detach_token})
      assert_receive {:DOWN, ^ref, :process, ^session, :normal}
    end
  end

  test "choosing Invisible or DND on another device also updates the suspended phone" do
    session = mobile("online")
    send_frame(session, 3, %{"status" => "idle", "afk" => true, "background" => true})
    detach(session)

    for {status, expected} <- [{"invisible", "offline"}, {"dnd", "dnd"}, {"online", "idle"}] do
      send(
        session,
        {:user_event,
         %DiscordyGateway.Domain.Events.EventEnvelope{
           id: "settings-" <> status,
           type: "USER_SETTINGS_UPDATE",
           data: %{"status" => status},
           target_user_ids: ["1"]
         }}
      )

      :sys.get_state(session)
      assert %{status: ^expected} = Fakes.get({:aggregate, "1"})
    end
  end
end
