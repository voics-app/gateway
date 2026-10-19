defmodule DiscordyGateway.Domain.Events.EventEnvelopeTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Events.EventEnvelope

  @raw %{
    "id" => "0192-uuid",
    "type" => "MESSAGE_CREATE",
    "v" => 1,
    "occurred_at" => "2026-10-16T10:00:00Z",
    "guild_id" => "100",
    "channel_id" => "10",
    "target_user_ids" => nil,
    "data" => %{"id" => "m1"}
  }

  test "parse/1 reads a P6 envelope" do
    assert {:ok, env} = EventEnvelope.parse(@raw)
    assert env.id == "0192-uuid"
    assert env.type == "MESSAGE_CREATE"
    assert env.guild_id == "100"
    assert env.data == %{"id" => "m1"}
  end

  test "parse/1 rejects envelopes without id, type or data" do
    assert EventEnvelope.parse(Map.delete(@raw, "id")) == {:error, :invalid_envelope}
    assert EventEnvelope.parse(Map.delete(@raw, "type")) == {:error, :invalid_envelope}
    assert EventEnvelope.parse(%{@raw | "data" => "x"}) == {:error, :invalid_envelope}
    assert EventEnvelope.parse("nope") == {:error, :invalid_envelope}
  end

  describe "route/1 (P6 gateway routing)" do
    test "guild events without targets fan out through the guild" do
      {:ok, env} = EventEnvelope.parse(@raw)
      assert EventEnvelope.route(env) == {:guild, "100"}
    end

    test "targeted events go to the listed users, even with a guild id" do
      {:ok, env} = EventEnvelope.parse(%{@raw | "target_user_ids" => ["1", "2"]})
      assert EventEnvelope.route(env) == {:users, ["1", "2"]}
    end

    test "events with neither guild nor targets are dropped" do
      {:ok, env} = EventEnvelope.parse(%{@raw | "guild_id" => nil})
      assert EventEnvelope.route(env) == :drop
    end
  end

  test "new/3 builds a publishable envelope" do
    env =
      EventEnvelope.new("VOICE_QUALITY", %{"x" => 1},
        id: "id-1",
        occurred_at: "2026-10-16T10:00:00Z",
        channel_id: "10"
      )

    assert EventEnvelope.to_map(env) == %{
             "id" => "id-1",
             "type" => "VOICE_QUALITY",
             "v" => 1,
             "occurred_at" => "2026-10-16T10:00:00Z",
             "guild_id" => nil,
             "channel_id" => "10",
             "target_user_ids" => nil,
             "data" => %{"x" => 1}
           }
  end
end
