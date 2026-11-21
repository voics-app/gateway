defmodule DiscordyGateway.Application.SendAccessTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Application.Relay.SendAccess

  test "requires the thread send bit for threads and fails closed on missing permissions" do
    assert :ok =
             SendAccess.authorize(%{
               "channel_type" => 11,
               "permissions" => "274877907968",
               "thread_locked" => false
             })

    assert {:error, :missing_permissions} =
             SendAccess.authorize(%{"channel_type" => 11, "permissions" => "3072"})

    assert {:error, :missing_permissions} = SendAccess.authorize(%{"channel_type" => 0})
  end

  test "locked or unknown thread state denies relay even for administrators" do
    for kind <- [10, 11, 12] do
      access = %{"channel_type" => kind, "permissions" => "274877907976"}
      assert {:error, :missing_permissions} = SendAccess.authorize(access)

      assert {:error, :missing_permissions} =
               SendAccess.authorize(Map.put(access, "thread_locked", true))

      assert :ok = SendAccess.authorize(Map.put(access, "thread_locked", false))
    end
  end
end
