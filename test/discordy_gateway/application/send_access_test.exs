defmodule DiscordyGateway.Application.SendAccessTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Application.Relay.SendAccess

  test "requires the thread send bit for threads and fails closed on missing permissions" do
    assert :ok = SendAccess.authorize(%{"channel_type" => 11, "permissions" => "274877907968"})

    assert {:error, :missing_permissions} =
             SendAccess.authorize(%{"channel_type" => 11, "permissions" => "3072"})

    assert {:error, :missing_permissions} = SendAccess.authorize(%{"channel_type" => 0})
  end
end
