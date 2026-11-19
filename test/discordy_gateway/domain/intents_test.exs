defmodule DiscordyGateway.Domain.IntentsTest do
  use ExUnit.Case, async: true
  import Bitwise

  alias DiscordyGateway.Domain.Intents

  test "all/0 covers the defined Discord intent bits" do
    assert Intents.all() == (1 <<< 17) - 1 ||| 1 <<< 24 ||| 1 <<< 25
  end

  describe "validate/1" do
    test "accepts any combination of defined bits" do
      assert Intents.validate(0) == :ok
      assert Intents.validate(513) == :ok
      assert Intents.validate(Intents.all()) == :ok
    end

    test "rejects undefined bits and negatives" do
      assert Intents.validate(1 <<< 17) == {:error, :invalid_intents}
      assert Intents.validate(-1) == {:error, :invalid_intents}
    end
  end

  describe "check_privileged/2" do
    test "allows non-privileged intents" do
      assert Intents.check_privileged(1 ||| 1 <<< 9, 0) == :ok
    end

    test "rejects privileged intents that are not approved" do
      assert Intents.check_privileged(1 <<< 15, 0) == {:error, :disallowed_intents}
      assert Intents.check_privileged(1 <<< 1, 1 <<< 8) == {:error, :disallowed_intents}
    end

    test "allows approved privileged intents" do
      assert Intents.check_privileged(1 <<< 1 ||| 1 <<< 8, 1 <<< 1 ||| 1 <<< 8) == :ok
    end
  end

  describe "required_for/2" do
    test "guild events map to guild intents" do
      assert Intents.required_for("GUILD_CREATE", :guild) == 1 <<< 0
      assert Intents.required_for("CHANNEL_UPDATE", :guild) == 1 <<< 0
      assert Intents.required_for("GUILD_MEMBER_ADD", :guild) == 1 <<< 1
      assert Intents.required_for("GUILD_BAN_ADD", :guild) == 1 <<< 2
      assert Intents.required_for("GUILD_EMOJIS_UPDATE", :guild) == 1 <<< 3

      for event <- ~w(SOUNDBOARD_SOUND_CREATE SOUNDBOARD_SOUND_UPDATE SOUNDBOARD_SOUND_DELETE) do
        assert Intents.required_for(event, :guild) == 1 <<< 3
        refute Intents.allows?(0, event, :guild)
      end

      assert Intents.required_for("INVITE_CREATE", :guild) == 1 <<< 6
      assert Intents.required_for("VOICE_STATE_UPDATE", :guild) == 1 <<< 7
      assert Intents.required_for("PRESENCE_UPDATE", :guild) == 1 <<< 8
      assert Intents.required_for("MESSAGE_CREATE", :guild) == 1 <<< 9
      assert Intents.required_for("MESSAGE_REACTION_ADD", :guild) == 1 <<< 10
      assert Intents.required_for("TYPING_START", :guild) == 1 <<< 11

      for event <- ~w(GUILD_SCHEDULED_EVENT_CREATE GUILD_SCHEDULED_EVENT_UPDATE
                      GUILD_SCHEDULED_EVENT_DELETE GUILD_SCHEDULED_EVENT_USER_ADD
                      GUILD_SCHEDULED_EVENT_USER_REMOVE) do
        assert Intents.required_for(event, :guild) == 1 <<< 16
        refute Intents.allows?(0, event, :guild)
        assert Intents.allows?(1 <<< 16, event, :guild)
      end
    end

    test "message events outside guilds map to direct message intents" do
      assert Intents.required_for("MESSAGE_CREATE", :direct) == 1 <<< 12
      assert Intents.required_for("MESSAGE_REACTION_REMOVE", :direct) == 1 <<< 13
      assert Intents.required_for("TYPING_START", :direct) == 1 <<< 14
    end

    test "ungated events need no intent" do
      assert Intents.required_for("READY", :direct) == 0
      assert Intents.required_for("INTERACTION_CREATE", :direct) == 0
      assert Intents.required_for("USER_UPDATE", :direct) == 0
      assert Intents.required_for("VOICE_SERVER_UPDATE", :guild) == 0
    end
  end

  test "allows?/3 checks the session's intents" do
    assert Intents.allows?(1 <<< 9, "MESSAGE_CREATE", :guild)
    refute Intents.allows?(1 <<< 0, "MESSAGE_CREATE", :guild)
    assert Intents.allows?(0, "INTERACTION_CREATE", :direct)
  end

  test "poll votes require the scope-specific poll intent" do
    for event <- ~w(MESSAGE_POLL_VOTE_ADD MESSAGE_POLL_VOTE_REMOVE) do
      assert Intents.validate(1 <<< 24 ||| 1 <<< 25) == :ok
      assert Intents.allows?(1 <<< 24, event, :guild)
      refute Intents.allows?(1 <<< 25, event, :guild)
      assert Intents.allows?(1 <<< 25, event, :direct)
      refute Intents.allows?(1 <<< 24, event, :direct)
      refute Intents.allows?(0, event, :guild)
      refute Intents.allows?(0, event, :direct)
    end
  end

  test "has?/2 tests a named intent" do
    assert Intents.has?(1 <<< 15, :message_content)
    refute Intents.has?(0, :message_content)
  end
end
