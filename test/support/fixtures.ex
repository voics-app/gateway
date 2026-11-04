defmodule DiscordyGateway.Fixtures do
  @moduledoc "Shared test fixtures."
  import Bitwise

  @view 1 <<< 10
  @send 1 <<< 11
  @connect 1 <<< 20

  def guild_payload(guild_id \\ "100") do
    %{
      "id" => guild_id,
      "name" => "Guild",
      "owner_id" => "1",
      "voice_region" => "eu-west",
      "roles" => [
        %{
          "id" => guild_id,
          "name" => "@everyone",
          "permissions" => Integer.to_string(@view ||| @send ||| @connect)
        },
        %{"id" => "300", "name" => "mod", "permissions" => "4"}
      ],
      "channels" => [
        %{"id" => "10", "type" => 0, "name" => "general", "permission_overwrites" => []},
        %{
          "id" => "11",
          "type" => 0,
          "name" => "staff",
          "permission_overwrites" => [
            %{"id" => guild_id, "type" => 0, "allow" => "0", "deny" => Integer.to_string(@view)},
            %{"id" => "300", "type" => 0, "allow" => Integer.to_string(@view), "deny" => "0"}
          ]
        },
        %{
          "id" => "12",
          "type" => 2,
          "name" => "voice",
          "permission_overwrites" => [],
          "voice_region" => "us-east"
        },
        %{"id" => "13", "type" => 11, "name" => "thread", "parent_id" => "11"}
      ],
      "members" => [
        %{"user" => %{"id" => "1", "username" => "owner"}, "roles" => []},
        %{"user" => %{"id" => "2", "username" => "alice"}, "nick" => "Ally", "roles" => ["300"]},
        %{"user" => %{"id" => "3", "username" => "bob"}, "roles" => []}
      ]
    }
  end

  @doc "A guild with `member_count` members (ids `u1`, `u2`, ...) shaped like the api sends them."
  def large_guild_payload(guild_id, member_count) do
    members =
      for n <- 1..member_count do
        %{
          "user" => %{
            "id" => "u#{n}",
            "username" => "member-#{n}",
            "global_name" => "Member Number #{n}",
            "avatar" => "a1b2c3d4e5f60718293a4b5c6d7e8f90",
            "discriminator" => "0"
          },
          "nick" => "nick-#{n}",
          "roles" => ["300"],
          "joined_at" => "2026-10-18T00:00:00Z"
        }
      end

    channels =
      for n <- 1..8 do
        %{"id" => "c#{n}", "type" => 0, "name" => "channel-#{n}", "permission_overwrites" => []}
      end

    guild_payload(guild_id) |> Map.put("members", members) |> Map.put("channels", channels)
  end
end
