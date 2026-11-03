defmodule DiscordyGateway.Domain.ActivitiesTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Activities

  defp custom(state \\ "busy"),
    do: %{"name" => "Custom Status", "type" => 4, "state" => state, "emoji" => nil}

  test "accepts the desktop client's custom status, with null emoji or emoji with an id" do
    assert {:ok, [%{"type" => 4, "state" => "busy"} = a]} = Activities.sanitize([custom()])
    refute Map.has_key?(a, "emoji")

    emoji = %{"name" => "x", "id" => "123"}

    assert {:ok, [%{"emoji" => %{"name" => "x", "id" => "123"}}]} =
             Activities.sanitize([%{custom() | "emoji" => emoji}])
  end

  test "accepts an empty list" do
    assert Activities.sanitize([]) == {:ok, []}
  end

  test "strips unknown fields, also inside emoji and timestamps" do
    raw = %{
      "name" => "Game",
      "type" => 0,
      "junk" => String.duplicate("x", 10_000),
      "emoji" => %{"name" => "a", "animated" => true, "extra" => 1},
      "timestamps" => %{"start" => 1, "end" => 2, "other" => 3}
    }

    assert {:ok, [clean]} = Activities.sanitize([raw])

    assert clean == %{
             "name" => "Game",
             "type" => 0,
             "emoji" => %{"name" => "a", "animated" => true},
             "timestamps" => %{"start" => 1, "end" => 2}
           }
  end

  test "rejects more than 5 activities" do
    game = %{"name" => "g", "type" => 0}
    assert {:ok, _} = Activities.sanitize(List.duplicate(game, 5))
    assert Activities.sanitize(List.duplicate(game, 6)) == {:error, :invalid_activities}
  end

  test "rejects more than one custom status" do
    assert Activities.sanitize([custom(), custom("again")]) == {:error, :invalid_activities}
  end

  test "rejects unknown types and non-integer types" do
    for type <- [-1, 6, 99, "0", nil] do
      assert Activities.sanitize([%{"name" => "g", "type" => type}]) ==
               {:error, :invalid_activities}
    end
  end

  test "caps name, state and details at 128 characters" do
    long = String.duplicate("a", 129)
    ok = String.duplicate("a", 128)
    assert {:ok, _} = Activities.sanitize([%{"name" => ok, "type" => 0, "state" => ok}])

    for field <- ~w(name state details) do
      assert Activities.sanitize([Map.put(%{"name" => "g", "type" => 0}, field, long)]) ==
               {:error, :invalid_activities}
    end
  end

  test "rejects wrongly shaped fields and entries" do
    bad = [
      "text",
      %{"type" => 0},
      %{"name" => "", "type" => 0},
      %{"name" => 1, "type" => 0},
      %{"name" => "g", "type" => 0, "state" => 5},
      %{"name" => "g", "type" => 0, "emoji" => "x"},
      %{"name" => "g", "type" => 0, "emoji" => %{"name" => String.duplicate("e", 200)}},
      %{"name" => "g", "type" => 0, "timestamps" => %{"start" => "now"}},
      %{"name" => "g", "type" => 0, "created_at" => -1},
      %{"name" => "g", "type" => 0, "url" => String.duplicate("u", 513)}
    ]

    for entry <- bad do
      assert Activities.sanitize([entry]) == {:error, :invalid_activities}, inspect(entry)
    end

    assert Activities.sanitize(%{"name" => "g"}) == {:error, :invalid_activities}
  end
end
