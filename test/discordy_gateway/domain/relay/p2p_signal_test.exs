defmodule DiscordyGateway.Domain.Relay.P2pSignalTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Relay.P2pSignal

  @signal %{
    channel_id: "c1",
    to_user_id: "u2",
    to_device_id: "d2",
    from_user_id: "u1",
    from_device_id: "d1",
    kind: "offer",
    payload: "sdp",
    identity_sig_b64: "c2ln"
  }

  @sender %{user_id: "u1", device_id: "d1"}

  test "authorize/4 accepts a genuine sender in a p2p chat with the recipient" do
    assert P2pSignal.authorize(@signal, @sender, "p2p", ["u1", "u2"]) == :ok
  end

  test "authorize/4 rejects spoofing, non-p2p chats and outsiders" do
    assert P2pSignal.authorize(@signal, %{user_id: "u9", device_id: "d1"}, "p2p", ["u1", "u2"]) ==
             {:error, :sender_mismatch}

    assert P2pSignal.authorize(@signal, %{user_id: "u1", device_id: "d9"}, "p2p", ["u1", "u2"]) ==
             {:error, :sender_mismatch}

    assert P2pSignal.authorize(@signal, @sender, "ephemeral", ["u1", "u2"]) == {:error, :not_p2p}
    assert P2pSignal.authorize(@signal, @sender, "p2p", ["u1"]) == {:error, :recipient_not_allowed}
  end

  test "to_payload/1 renders the relayed frame" do
    payload = P2pSignal.to_payload(@signal)
    assert payload["kind"] == "offer"
    assert payload["identity_sig_b64"] == "c2ln"
    assert payload["from_device_id"] == "d1"
  end

  test "failure_payload/1 reports peer_offline" do
    assert P2pSignal.failure_payload(@signal) == %{
             "error" => "peer_offline",
             "channel_id" => "c1",
             "to_user_id" => "u2",
             "to_device_id" => "d2",
             "kind" => "offer"
           }
  end
end
