defmodule DiscordyGateway.Domain.Protocol.CodecTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Protocol.Codec

  defp frame(op, d), do: %{"op" => op, "d" => d, "s" => nil, "t" => nil}

  describe "decode_inbound/1 frame shape" do
    test "rejects non-map payloads with decode error 4002" do
      assert Codec.decode_inbound([1, 2]) == {:error, 4002}
      assert Codec.decode_inbound("x") == {:error, 4002}
    end

    test "rejects a missing or non-integer op with 4002" do
      assert Codec.decode_inbound(%{"d" => nil}) == {:error, 4002}
      assert Codec.decode_inbound(%{"op" => "1", "d" => nil}) == {:error, 4002}
    end

    test "rejects unknown opcodes with 4001" do
      assert Codec.decode_inbound(frame(5, nil)) == {:error, 4001}
      assert Codec.decode_inbound(frame(99, nil)) == {:error, 4001}
    end

    test "rejects server-only opcodes sent by the client with 4001" do
      for op <- [0, 7, 9, 10, 11] do
        assert Codec.decode_inbound(frame(op, nil)) == {:error, 4001}
      end
    end
  end

  describe "op 1 HEARTBEAT" do
    test "accepts null and integer sequence" do
      assert Codec.decode_inbound(frame(1, nil)) == {:ok, {:heartbeat, nil}}
      assert Codec.decode_inbound(frame(1, 42)) == {:ok, {:heartbeat, 42}}
    end

    test "rejects a non-integer sequence" do
      assert Codec.decode_inbound(frame(1, "42")) == {:error, 4002}
    end
  end

  describe "op 2 IDENTIFY" do
    @identify %{
      "token" => "jwt",
      "device_id" => "123",
      "intents" => 513,
      "properties" => %{"os" => "windows", "client" => "discordy", "version" => "0.1.0"}
    }

    test "decodes a full payload" do
      assert {:ok, {:identify, identify}} = Codec.decode_inbound(frame(2, @identify))
      assert identify.token == "jwt"
      assert identify.device_id == "123"
      assert identify.intents == 513
      assert identify.properties == %{os: "windows", client: "discordy", version: "0.1.0"}
      assert identify.presence == nil
      assert identify.passive == false
    end

    test "decodes an optional initial presence" do
      payload =
        Map.put(@identify, "presence", %{"status" => "dnd", "activities" => [], "afk" => false})

      assert {:ok, {:identify, identify}} = Codec.decode_inbound(frame(2, payload))
      assert identify.presence == %{status: "dnd", activities: [], afk: false, background: false}
    end

    test "validates optional passive mode" do
      assert {:ok, {:identify, %{passive: true}}} =
               Codec.decode_inbound(frame(2, Map.put(@identify, "passive", true)))

      assert Codec.decode_inbound(frame(2, Map.put(@identify, "passive", nil))) == {:error, 4002}
    end

    test "rejects a missing token" do
      assert Codec.decode_inbound(frame(2, Map.delete(@identify, "token"))) == {:error, 4002}
    end

    test "rejects non-integer intents with 4013" do
      assert Codec.decode_inbound(frame(2, %{@identify | "intents" => "all"})) == {:error, 4013}
    end
  end

  describe "op 3 PRESENCE_UPDATE" do
    test "decodes statuses" do
      for status <- ~w(online idle dnd invisible) do
        d = %{"status" => status, "activities" => [%{"name" => "x", "type" => 0}], "afk" => true}
        assert {:ok, {:presence_update, p}} = Codec.decode_inbound(frame(3, d))

        assert p == %{
                 status: status,
                 activities: [%{"name" => "x", "type" => 0}],
                 afk: true,
                 background: false
               }
      end
    end

    test "rejects unknown statuses" do
      assert Codec.decode_inbound(frame(3, %{"status" => "away", "activities" => []})) ==
               {:error, 4002}
    end
  end

  test "session mode requires a boolean" do
    assert {:ok, {:session_mode, %{passive: true}}} =
             Codec.decode_inbound(frame(27, %{"passive" => true}))

    assert {:ok, {:session_mode, %{passive: false}}} =
             Codec.decode_inbound(frame(27, %{"passive" => false}))

    assert Codec.decode_inbound(frame(27, %{})) == {:error, 4002}
    assert Codec.decode_inbound(frame(27, %{"passive" => nil})) == {:error, 4002}
  end

  test "decodes call ring with unique targets and rejects empty or oversized lists" do
    assert {:ok, {:call_ring, %{channel_id: "c", user_ids: ["1", "2"]}}} =
             Codec.decode_inbound(frame(26, %{"channel_id" => "c", "user_ids" => ~w(1 2 1)}))

    assert Codec.decode_inbound(frame(26, %{"channel_id" => "c", "user_ids" => []})) ==
             {:error, 4002}

    many = Enum.map(1..11, &Integer.to_string/1)

    assert Codec.decode_inbound(frame(26, %{"channel_id" => "c", "user_ids" => many})) ==
             {:error, 4002}
  end

  describe "op 3 activity validation" do
    test "sanitises activities and rejects oversized ones with 4002" do
      d = %{"status" => "online", "afk" => false}
      activity = %{"name" => "x", "type" => 0, "junk" => 1}

      assert {:ok, {:presence_update, p}} =
               Codec.decode_inbound(frame(3, Map.put(d, "activities", [activity])))

      assert p.activities == [%{"name" => "x", "type" => 0}]

      too_long = [%{"name" => "x", "type" => 4, "state" => String.duplicate("a", 129)}]
      assert Codec.decode_inbound(frame(3, Map.put(d, "activities", too_long))) == {:error, 4002}
    end
  end

  describe "op 4 VOICE_STATE_UPDATE" do
    test "decodes join and leave" do
      d = %{"guild_id" => "1", "channel_id" => "2", "self_mute" => true, "self_deaf" => false}
      assert {:ok, {:voice_state_update, v}} = Codec.decode_inbound(frame(4, d))

      assert v == %{
               guild_id: "1",
               channel_id: "2",
               self_mute: true,
               self_deaf: false,
               self_video: false,
               self_stream: false,
               request_to_speak: :unchanged
             }

      assert {:ok, {:voice_state_update, %{request_to_speak: "2026-11-20T01:00:00Z"}}} =
               Codec.decode_inbound(
                 frame(4, Map.put(d, "request_to_speak_timestamp", "2026-11-20T01:00:00Z"))
               )

      invalid = frame(4, Map.put(d, "request_to_speak_timestamp", "now"))
      assert Codec.decode_inbound(invalid) == {:error, 4002}

      leave = %{
        "guild_id" => nil,
        "channel_id" => nil,
        "self_mute" => false,
        "self_deaf" => false
      }

      assert {:ok, {:voice_state_update, %{channel_id: nil}}} =
               Codec.decode_inbound(frame(4, leave))
    end

    test "rejects non-boolean flags" do
      d = %{"guild_id" => "1", "channel_id" => "2", "self_mute" => "yes", "self_deaf" => false}
      assert Codec.decode_inbound(frame(4, d)) == {:error, 4002}
    end
  end

  describe "op 6 RESUME" do
    test "decodes token, session and seq" do
      d = %{"token" => "t", "session_id" => "s", "seq" => 7}

      assert Codec.decode_inbound(frame(6, d)) ==
               {:ok, {:resume, %{token: "t", session_id: "s", seq: 7}}}
    end

    test "rejects a negative seq" do
      assert Codec.decode_inbound(frame(6, %{"token" => "t", "session_id" => "s", "seq" => -1})) ==
               {:error, 4002}
    end
  end

  describe "op 8 REQUEST_GUILD_MEMBERS" do
    test "decodes a query request" do
      d = %{"guild_id" => "1", "query" => "ab", "limit" => 10, "nonce" => "n"}
      assert {:ok, {:request_guild_members, r}} = Codec.decode_inbound(frame(8, d))

      assert r == %{
               guild_id: "1",
               query: "ab",
               limit: 10,
               user_ids: nil,
               nonce: "n",
               presences: false
             }
    end

    test "decodes a user_ids request" do
      d = %{"guild_id" => "1", "limit" => 0, "user_ids" => ["2", "3"]}

      assert {:ok, {:request_guild_members, %{user_ids: ["2", "3"], query: nil}}} =
               Codec.decode_inbound(frame(8, d))
    end

    test "requires query or user_ids" do
      assert Codec.decode_inbound(frame(8, %{"guild_id" => "1", "limit" => 0})) == {:error, 4002}
    end
  end

  describe "op 20 E2EE_ENVELOPE" do
    @envelope %{
      "channel_id" => "1",
      "message_id" => "m1",
      "sender_device_id" => "d1",
      "recipients" => [%{"user_id" => "u2", "device_id" => "d2", "ciphertext_b64" => "AAEC"}],
      "mls_epoch" => 3,
      "content_type" => "application"
    }

    test "decodes an envelope" do
      assert {:ok, {:e2ee_envelope, e}} = Codec.decode_inbound(frame(20, @envelope))
      assert e.channel_id == "1"
      assert e.recipients == [%{user_id: "u2", device_id: "d2", ciphertext_b64: "AAEC"}]
      assert e.mls_epoch == 3
      assert e.content_type == "application"
    end

    test "rejects invalid base64 ciphertext and unknown content types" do
      bad_b64 =
        put_in(@envelope, ["recipients"], [
          %{"user_id" => "u", "device_id" => "d", "ciphertext_b64" => "%%"}
        ])

      assert Codec.decode_inbound(frame(20, bad_b64)) == {:error, 4002}

      assert Codec.decode_inbound(frame(20, %{@envelope | "content_type" => "x"})) ==
               {:error, 4002}
    end

    test "rejects an empty recipient list" do
      assert Codec.decode_inbound(frame(20, %{@envelope | "recipients" => []})) == {:error, 4002}
    end
  end

  describe "op 21 E2EE_ACK" do
    test "decodes message ids" do
      assert Codec.decode_inbound(frame(21, %{"message_ids" => ["a", "b"]})) ==
               {:ok, {:e2ee_ack, %{message_ids: ["a", "b"]}}}
    end

    test "rejects non-string ids" do
      assert Codec.decode_inbound(frame(21, %{"message_ids" => [1]})) == {:error, 4002}
    end
  end

  describe "op 22 P2P_SIGNAL" do
    @signal %{
      "channel_id" => "c",
      "to_user_id" => "u2",
      "to_device_id" => "d2",
      "from_user_id" => "u1",
      "from_device_id" => "d1",
      "kind" => "offer",
      "payload" => "v=0...",
      "identity_sig_b64" => "c2ln"
    }

    test "decodes every kind" do
      for kind <- ~w(offer answer ice bye) do
        assert {:ok, {:p2p_signal, s}} =
                 Codec.decode_inbound(frame(22, %{@signal | "kind" => kind}))

        assert s.kind == kind
        assert s.to_device_id == "d2"
      end
    end

    test "rejects unknown kinds" do
      assert Codec.decode_inbound(frame(22, %{@signal | "kind" => "hello"})) == {:error, 4002}
    end
  end

  describe "op 23 VOICE_TELEMETRY" do
    test "decodes numeric samples" do
      d = %{
        "channel_id" => "c",
        "jitter_ms" => 3.5,
        "loss_pct" => 0,
        "rtt_ms" => 40,
        "bitrate_kbps" => 64
      }

      assert {:ok, {:voice_telemetry, t}} = Codec.decode_inbound(frame(23, d))
      assert t == %{channel_id: "c", jitter_ms: 3.5, loss_pct: 0, rtt_ms: 40, bitrate_kbps: 64}
    end

    test "rejects negative or non-numeric samples" do
      d = %{
        "channel_id" => "c",
        "jitter_ms" => -1,
        "loss_pct" => 0,
        "rtt_ms" => 40,
        "bitrate_kbps" => 64
      }

      assert Codec.decode_inbound(frame(23, d)) == {:error, 4002}
    end
  end

  describe "op 24 WATCH_TOGETHER" do
    test "decodes load with url" do
      d = %{"channel_id" => "c", "action" => "load", "url" => "https://v", "position_ms" => 0}
      assert {:ok, {:watch_together, w}} = Codec.decode_inbound(frame(24, d))
      assert w == %{channel_id: "c", action: "load", url: "https://v", position_ms: 0}
    end

    test "requires a url for load" do
      d = %{"channel_id" => "c", "action" => "load", "position_ms" => 0}
      assert Codec.decode_inbound(frame(24, d)) == {:error, 4002}
    end

    test "rejects unknown actions" do
      d = %{"channel_id" => "c", "action" => "rewind", "position_ms" => 0}
      assert Codec.decode_inbound(frame(24, d)) == {:error, 4002}
    end
  end

  describe "outbound frames" do
    test "hello" do
      assert Codec.hello(41_250) == %{
               "op" => 10,
               "d" => %{"heartbeat_interval" => 41_250},
               "s" => nil,
               "t" => nil
             }
    end

    test "heartbeat ack, heartbeat request, reconnect" do
      assert Codec.heartbeat_ack() == %{"op" => 11, "d" => nil, "s" => nil, "t" => nil}
      assert Codec.heartbeat_request(5) == %{"op" => 1, "d" => 5, "s" => nil, "t" => nil}
      assert Codec.reconnect() == %{"op" => 7, "d" => nil, "s" => nil, "t" => nil}
    end

    test "invalid session carries the resumable flag" do
      assert Codec.invalid_session(true) == %{"op" => 9, "d" => true, "s" => nil, "t" => nil}
      assert Codec.invalid_session(false)["d"] == false
    end

    test "relay frames for ops 20, 22 and 24" do
      assert Codec.relay(:e2ee_envelope, %{"a" => 1}) == %{
               "op" => 20,
               "d" => %{"a" => 1},
               "s" => nil,
               "t" => nil
             }

      assert Codec.relay(:p2p_signal, %{})["op"] == 22
      assert Codec.relay(:watch_together, %{})["op"] == 24
    end

    test "dispatch frame as iodata around a pre-encoded payload" do
      iodata = Codec.dispatch_iodata(12, "MESSAGE_CREATE", ~s({"id":"1"}))

      assert IO.iodata_to_binary(iodata) ==
               ~s({"op":0,"s":12,"t":"MESSAGE_CREATE","d":{"id":"1"}})
    end
  end
end
