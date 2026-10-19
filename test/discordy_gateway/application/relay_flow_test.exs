defmodule DiscordyGateway.Application.RelayFlowTest do
  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Ports
  alias DiscordyGateway.Application.Relay.InboxSweeper
  alias DiscordyGateway.Application.UsageTicker

  defp access(channel, mode, users \\ ~w(2 3)) do
    for user <- users do
      Fakes.put(
        {:access, channel, user},
        {:ok,
         %{
           "guild_id" => nil,
           "delivery_mode" => mode,
           "recipient_ids" => users,
           "channel_type" => 1
         }}
      )
    end
  end

  defp envelope(overrides \\ %{}) do
    Map.merge(
      %{
        "channel_id" => "c1",
        "message_id" => "m1",
        "sender_device_id" => "dev-2",
        "recipients" => [
          %{"user_id" => "3", "device_id" => "dev-3", "ciphertext_b64" => "AAEC"},
          %{"user_id" => "3", "device_id" => "dev-3b", "ciphertext_b64" => "AwQF"}
        ],
        "mls_epoch" => 4,
        "content_type" => "application"
      },
      overrides
    )
  end

  describe "E2EE relay" do
    test "delivers each online device only its own entry and stores every entry in the inbox" do
      access("c1", "ephemeral")
      sender = online("2")
      receiver = online("3")

      send_frame(sender.session, 20, envelope())

      assert %{
               "op" => 20,
               "d" => %{
                 "recipients" => [%{"device_id" => "dev-3", "ciphertext_b64" => "AAEC"}],
                 "message_id" => "m1"
               }
             } =
               await_op(receiver, 20)

      assert {:ok, [{"m1", _}]} = Ports.inbox_store().list("dev-3")
      assert {:ok, [{"m1", _}]} = Ports.inbox_store().list("dev-3b")
    end

    test "envelopes reach recipients in the order the sender sent them" do
      access("c1", "ephemeral")
      sender = online("2")
      receiver = online("3")

      # A welcome to many devices takes longer to store than the message that follows it.
      many =
        for n <- 1..300,
            do: %{"user_id" => "3", "device_id" => "old-#{n}", "ciphertext_b64" => "AA"}

      welcome = %{
        "message_id" => "w1",
        "content_type" => "welcome",
        "recipients" => [
          %{"user_id" => "3", "device_id" => "dev-3", "ciphertext_b64" => "AA"} | many
        ]
      }

      send_frame(sender.session, 20, envelope(welcome))
      send_frame(sender.session, 20, envelope(%{"message_id" => "m2"}))

      assert %{"d" => %{"message_id" => "w1"}} = await_op(receiver, 20, 2_000)
      assert %{"d" => %{"message_id" => "m2"}} = await_op(receiver, 20, 2_000)
    end

    test "ACK deletes the entries from the device's inbox" do
      access("c1", "ephemeral")
      sender = online("2")
      receiver = online("3")
      send_frame(sender.session, 20, envelope())
      await_op(receiver, 20)

      send_frame(receiver.session, 21, %{"message_ids" => ["m1"]})
      Process.sleep(50)
      assert {:ok, []} = Ports.inbox_store().list("dev-3")
      assert {:ok, [{"m1", _}]} = Ports.inbox_store().list("dev-3b")
    end

    test "pending entries are replayed after READY" do
      access("c1", "ephemeral")
      sender = online("2")
      send_frame(sender.session, 20, envelope())
      Process.sleep(50)

      late = client("3")
      identify(late.session, "3")

      assert %{"d" => %{"message_id" => "m1", "recipients" => [%{"device_id" => "dev-3"}]}} =
               await_op(late, 20)
    end

    test "standard-delivery channels, spoofed senders and foreign recipients are rejected" do
      access("c1", "standard")
      sender = online("2")
      send_frame(sender.session, 20, envelope())

      assert %{"reason" => "not_ephemeral", "message_id" => "m1"} =
               await(sender, "E2EE_ENVELOPE_REJECTED")

      access("c1", "ephemeral")
      send_frame(sender.session, 20, envelope(%{"sender_device_id" => "dev-evil"}))
      assert %{"reason" => "sender_device_mismatch"} = await(sender, "E2EE_ENVELOPE_REJECTED")

      foreign = [%{"user_id" => "66", "device_id" => "d", "ciphertext_b64" => "AAEC"}]
      send_frame(sender.session, 20, envelope(%{"recipients" => foreign}))
      assert %{"reason" => "recipient_not_allowed"} = await(sender, "E2EE_ENVELOPE_REJECTED")
      assert {:ok, []} = Ports.inbox_store().list("d")
    end

    test "a user who cannot access the channel is rejected" do
      sender = online("2")
      send_frame(sender.session, 20, envelope())
      assert %{"reason" => "forbidden"} = await(sender, "E2EE_ENVELOPE_REJECTED")
    end

    test "the sweeper deletes expired entries" do
      Ports.inbox_store().store("dev-9", "old", "{}", System.os_time(:millisecond) - 1)
      Ports.inbox_store().store("dev-9", "fresh", "{}", System.os_time(:millisecond) + 60_000)
      {:ok, sweeper} = start_supervised({InboxSweeper, name: :test_sweeper, interval_ms: 60_000})

      assert InboxSweeper.sweep_now(sweeper) == 1
      assert {:ok, [{"fresh", _}]} = Ports.inbox_store().list("dev-9")
    end
  end

  describe "P2P signalling" do
    defp signal(overrides \\ %{}) do
      Map.merge(
        %{
          "channel_id" => "c2",
          "to_user_id" => "3",
          "to_device_id" => "dev-3",
          "from_user_id" => "2",
          "from_device_id" => "dev-2",
          "kind" => "offer",
          "payload" => "sdp...",
          "identity_sig_b64" => "AAEC"
        },
        overrides
      )
    end

    test "is relayed to the online target device and never stored" do
      access("c2", "p2p")
      sender = online("2")
      receiver = online("3")

      send_frame(sender.session, 22, signal())

      assert %{
               "op" => 22,
               "d" => %{"kind" => "offer", "from_device_id" => "dev-2", "payload" => "sdp..."}
             } = await_op(receiver, 22)

      assert {:ok, []} = Ports.inbox_store().list("dev-3")
    end

    test "an offline peer yields P2P_SIGNAL_FAILED peer_offline" do
      access("c2", "p2p")
      sender = online("2")
      send_frame(sender.session, 22, signal())

      assert %{"error" => "peer_offline", "to_device_id" => "dev-3", "kind" => "offer"} =
               await(sender, "P2P_SIGNAL_FAILED")
    end

    test "a device that belongs to another user is not a valid target" do
      access("c2", "p2p")
      sender = online("2")
      _other = online("4", %{"device_id" => "dev-3"})
      send_frame(sender.session, 22, signal())
      assert %{"error" => "peer_offline"} = await(sender, "P2P_SIGNAL_FAILED")
    end

    test "non-p2p channels and spoofed senders fail" do
      access("c2", "ephemeral")
      sender = online("2")
      send_frame(sender.session, 22, signal())
      assert %{"error" => "peer_offline"} = await(sender, "P2P_SIGNAL_FAILED")

      access("c2", "p2p")
      send_frame(sender.session, 22, signal(%{"from_user_id" => "3"}))
      assert %{"error" => "peer_offline"} = await(sender, "P2P_SIGNAL_FAILED")
    end
  end

  describe "telemetry" do
    test "VOICE_TELEMETRY is published as VOICE_QUALITY on the telemetry topic" do
      Fakes.put(:voice_nodes, [voice_node("a")])
      access("dmt1", "standard")
      client = online("2")

      send_frame(client.session, 4, %{
        "guild_id" => nil,
        "channel_id" => "dmt1",
        "self_mute" => false,
        "self_deaf" => false
      })

      await(client, "VOICE_SERVER_UPDATE")

      send_frame(client.session, 23, %{
        "channel_id" => "dmt1",
        "jitter_ms" => 4.5,
        "loss_pct" => 0.2,
        "rtt_ms" => 31,
        "bitrate_kbps" => 64
      })

      Process.sleep(50)

      assert [{"discordy.telemetry.v1", "2", envelope}] = Fakes.calls(:published)

      assert %{
               "type" => "VOICE_QUALITY",
               "channel_id" => "dmt1",
               "data" => %{"source" => "client", "rtt_ms" => 31}
             } = envelope
    end

    test "samples from a session that is not in voice are ignored" do
      client = online("2")

      send_frame(client.session, 23, %{
        "channel_id" => "dmt1",
        "jitter_ms" => 1,
        "loss_pct" => 0,
        "rtt_ms" => 1,
        "bitrate_kbps" => 1
      })

      Process.sleep(50)
      assert Fakes.calls(:published) == []
    end

    test "the usage ticker publishes USAGE_TICK for this node" do
      {:ok, ticker} = start_supervised({UsageTicker, name: :test_ticker, interval_ms: 60_000})
      online("2")
      assert :ok = UsageTicker.tick_now(ticker)

      assert [{"discordy.telemetry.v1", key, %{"type" => "USAGE_TICK", "data" => data}}] =
               Fakes.calls(:published)

      assert data["sessions_online"] >= 1
      assert key == Atom.to_string(node())
    end
  end
end
