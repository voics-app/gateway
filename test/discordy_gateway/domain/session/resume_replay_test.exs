defmodule DiscordyGateway.Domain.Session.ResumeReplayTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Session.ResumeReplay

  defp entries(range), do: Enum.map(range, &%{seq: &1, t: "E", d: "{}"})

  test "returns the entries after the client's seq" do
    assert {:ok, replay} = ResumeReplay.select(entries(1..5), 3, 5)
    assert Enum.map(replay, & &1.seq) == [4, 5]
  end

  test "nothing to replay when the client is up to date" do
    assert ResumeReplay.select(entries(1..5), 5, 5) == {:ok, []}
  end

  test "a seq beyond the session's current seq is invalid" do
    assert ResumeReplay.select(entries(1..5), 6, 5) == {:error, :invalid_seq}
  end

  test "a gap between the client's seq and the oldest buffered entry is not resumable" do
    assert ResumeReplay.select(entries(10..12), 5, 12) == {:error, :gap}
  end

  test "no gap when the oldest buffered entry directly follows the client's seq" do
    assert {:ok, replay} = ResumeReplay.select(entries(6..8), 5, 8)
    assert length(replay) == 3
  end

  test "an empty buffer with an older seq is a gap" do
    assert ResumeReplay.select([], 2, 4) == {:error, :gap}
  end

  test "entries are sorted by seq whatever the storage order" do
    shuffled = Enum.shuffle(entries(1..4))
    assert {:ok, replay} = ResumeReplay.select(shuffled, 1, 4)
    assert Enum.map(replay, & &1.seq) == [2, 3, 4]
  end
end
