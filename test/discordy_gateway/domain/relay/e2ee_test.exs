defmodule DiscordyGateway.Domain.Relay.E2eeTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Relay.E2ee

  @envelope %{
    channel_id: "c1",
    message_id: "m1",
    sender_device_id: "d1",
    recipients: [
      %{user_id: "u2", device_id: "d2", ciphertext_b64: "AA"},
      %{user_id: "u3", device_id: "d3", ciphertext_b64: "AQ"}
    ],
    mls_epoch: 4,
    content_type: "application"
  }

  @sender %{user_id: "u1", device_id: "d1"}

  describe "authorize/4" do
    test "accepts a sender on its own device with allowed recipients" do
      assert E2ee.authorize(@envelope, @sender, "ephemeral", MapSet.new(["u1", "u2", "u3"])) ==
               :ok
    end

    test "accepts p2p chats, which use MLS envelopes for group state" do
      assert E2ee.authorize(@envelope, @sender, "p2p", MapSet.new(["u2", "u3"])) == :ok
    end

    test "rejects a spoofed sender device" do
      assert E2ee.authorize(
               @envelope,
               %{@sender | device_id: "dX"},
               "ephemeral",
               MapSet.new(["u2", "u3"])
             ) == {:error, :sender_device_mismatch}
    end

    test "rejects standard channels" do
      assert E2ee.authorize(@envelope, @sender, "standard", MapSet.new(["u2", "u3"])) ==
               {:error, :not_ephemeral}
    end

    test "rejects recipients outside the channel" do
      assert E2ee.authorize(@envelope, @sender, "ephemeral", MapSet.new(["u2"])) ==
               {:error, :recipient_not_allowed}
    end
  end

  test "per_recipient/1 yields one payload per device carrying only its entry" do
    [{"d2", "u2", p2}, {"d3", "u3", p3}] = E2ee.per_recipient(@envelope)

    assert p2 == %{
             "channel_id" => "c1",
             "message_id" => "m1",
             "sender_device_id" => "d1",
             "recipients" => [%{"user_id" => "u2", "device_id" => "d2", "ciphertext_b64" => "AA"}],
             "mls_epoch" => 4,
             "content_type" => "application"
           }

    assert [%{"device_id" => "d3"}] = p3["recipients"]
  end
end
