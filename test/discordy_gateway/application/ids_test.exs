defmodule DiscordyGateway.Application.IdsTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Application.Ids

  @uuid ~r/^[0-9a-f]{8}-[0-9a-f]{4}-([0-9a-f])[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

  test "uuid_v4/0 has version 4" do
    assert [_, "4"] = Regex.run(@uuid, Ids.uuid_v4())
  end

  test "uuid_v7/1 has version 7 and embeds the timestamp" do
    id = Ids.uuid_v7(1_767_225_600_000)
    assert [_, "7"] = Regex.run(@uuid, id)
    hex = id |> String.replace("-", "") |> String.slice(0, 12)
    assert String.to_integer(hex, 16) == 1_767_225_600_000
  end

  test "uuid_v7 ids sort by time" do
    assert Ids.uuid_v7(1_000) < Ids.uuid_v7(2_000)
  end
end
