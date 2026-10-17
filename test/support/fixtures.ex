defmodule DiscordyGateway.Fixtures do
  @moduledoc "Shared test fixtures."
  import Bitwise

  @view 1 <<< 10
  @send 1 <<< 11
  @connect 1 <<< 20

  def guild_payload do
    %{
      "id" => "100",
      "name" => "Guild",
      "owner_id" => "1",
      "voice_region" => "eu-west",
      "roles" => [
        %{
          "id" => "100",
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
            %{"id" => "100", "type" => 0, "allow" => "0", "deny" => Integer.to_string(@view)},
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
end
