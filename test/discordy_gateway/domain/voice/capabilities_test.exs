defmodule DiscordyGateway.Domain.Voice.CapabilitiesTest do
  use ExUnit.Case, async: true

  import Bitwise
  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Domain.Voice.Capabilities

  defp permission(names), do: Enum.reduce(names, 0, &bor(Contracts.permission(&1), &2))

  test "connect, suppression and independent media bits determine the complete grant" do
    all = permission([:view_channel, :connect, :speak, :stream, :use_soundboard])

    assert Capabilities.permissions(all, false) == %{
             "can_publish_audio" => true,
             "can_publish_video" => true,
             "can_publish_soundboard" => true
           }

    assert Enum.all?(Map.values(Capabilities.permissions(all, true)), &(&1 == false))
    no_speak = permission([:view_channel, :connect, :stream, :use_soundboard])
    assert Capabilities.permissions(no_speak, false)["can_publish_video"]
    refute Capabilities.permissions(no_speak, false)["can_publish_soundboard"]

    refute Capabilities.permissions(permission([:view_channel, :stream]), false)[
             "can_publish_video"
           ]
  end

  test "grants are session and lease bound, acknowledged exactly, and renewed before expiry" do
    state = Capabilities.new("session", "node", 42)
    assert Capabilities.expired?(state, 1_000)
    assert Capabilities.renew_due?(state, 1_000, 5_000)
    assert {:ok, pending, command} = Capabilities.prepare(state, 0, false, 1_000, 15_000)
    assert command["revision"] == 1
    assert command["registration_lease"] == 42
    assert {:error, :invalid_ttl} = Capabilities.prepare(state, 0, false, 1_000, 15_001)
    assert {:error, :pending} = Capabilities.prepare(pending, 0, false, 1_001, 15_000)

    assert {:error, :mismatch, %{applied: nil, pending: nil}, :disconnect} =
             Capabilities.acknowledge(pending, Map.put(command, "node_id", "other"), 1_001)

    assert {:error, :expired, %{applied: nil, pending: nil}, :disconnect} =
             Capabilities.acknowledge(pending, command, 16_000)

    assert {:ok, applied} = Capabilities.acknowledge(pending, command, 1_001)
    refute Capabilities.expired?(applied, 15_999)
    assert Capabilities.expired?(applied, 16_000)
    refute Capabilities.renew_due?(applied, 10_999, 5_000)
    assert Capabilities.renew_due?(applied, 11_000, 5_000)

    assert {:ok, next, %{"revision" => 2}} =
             Capabilities.prepare(applied, 0, false, 11_000, 15_000)

    assert {%{pending: nil, applied: nil}, :disconnect} = Capabilities.fail(next)
  end
end

