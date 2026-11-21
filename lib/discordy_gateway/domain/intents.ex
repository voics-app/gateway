defmodule DiscordyGateway.Domain.Intents do
  @moduledoc "Gateway intents (P7): validation, privileged gating and per-event requirements."

  import Bitwise

  alias DiscordyGateway.Contracts

  @all Contracts.intents() |> Map.values() |> Enum.reduce(0, &bor/2)
  @privileged Contracts.intent(:guild_members) ||| Contracts.intent(:guild_presences) |||
                Contracts.intent(:message_content)

  @guild_events %{
    guilds: ~w(GUILD_CREATE GUILD_UPDATE GUILD_DELETE GUILD_ROLE_CREATE GUILD_ROLE_UPDATE
               GUILD_ROLE_DELETE CHANNEL_CREATE CHANNEL_UPDATE CHANNEL_DELETE CHANNEL_PINS_UPDATE
               THREAD_CREATE THREAD_UPDATE THREAD_DELETE THREAD_MEMBERS_UPDATE),
    guild_members: ~w(GUILD_MEMBER_ADD GUILD_MEMBER_UPDATE GUILD_MEMBER_REMOVE),
    guild_moderation: ~w(GUILD_BAN_ADD GUILD_BAN_REMOVE AUDIT_LOG_ENTRY_CREATE),
    guild_emojis:
      ~w(GUILD_EMOJIS_UPDATE SOUNDBOARD_SOUND_CREATE SOUNDBOARD_SOUND_UPDATE SOUNDBOARD_SOUND_DELETE),
    guild_invites: ~w(INVITE_CREATE INVITE_DELETE),
    guild_voice_states: ~w(VOICE_STATE_UPDATE),
    guild_presences: ~w(PRESENCE_UPDATE),
    guild_messages: ~w(MESSAGE_CREATE MESSAGE_UPDATE MESSAGE_DELETE MESSAGE_DELETE_BULK),
    guild_message_reactions:
      ~w(MESSAGE_REACTION_ADD MESSAGE_REACTION_REMOVE MESSAGE_REACTION_REMOVE_ALL),
    guild_message_typing: ~w(TYPING_START),
    guild_scheduled_events:
      ~w(GUILD_SCHEDULED_EVENT_CREATE GUILD_SCHEDULED_EVENT_UPDATE GUILD_SCHEDULED_EVENT_DELETE
         GUILD_SCHEDULED_EVENT_USER_ADD GUILD_SCHEDULED_EVENT_USER_REMOVE)
  }

  @direct_events %{
    direct_messages: ~w(MESSAGE_CREATE MESSAGE_UPDATE MESSAGE_DELETE MESSAGE_DELETE_BULK
                        CHANNEL_PINS_UPDATE),
    direct_message_reactions:
      ~w(MESSAGE_REACTION_ADD MESSAGE_REACTION_REMOVE MESSAGE_REACTION_REMOVE_ALL),
    direct_message_typing: ~w(TYPING_START)
  }

  @guild_requirements for {intent, events} <- @guild_events,
                          event <- events,
                          into: %{},
                          do: {event, Contracts.intent(intent)}

  @direct_requirements for {intent, events} <- @direct_events,
                           event <- events,
                           into: %{},
                           do: {event, Contracts.intent(intent)}

  @type scope :: :guild | :direct

  @doc "Every defined intent bit; user sessions implicitly get all of them."
  @spec all() :: non_neg_integer()
  def all, do: @all

  @doc "Validates that `intents` only uses defined bits (close code 4013 otherwise)."
  @spec validate(integer()) :: :ok | {:error, :invalid_intents}
  def validate(intents)
      when is_integer(intents) and intents >= 0 and (intents &&& bnot(@all)) == 0,
      do: :ok

  def validate(_intents), do: {:error, :invalid_intents}

  @doc "Rejects privileged intents that the application is not approved for (close code 4014)."
  @spec check_privileged(non_neg_integer(), non_neg_integer()) ::
          :ok | {:error, :disallowed_intents}
  def check_privileged(requested, approved) do
    if (requested &&& @privileged &&& bnot(approved)) == 0,
      do: :ok,
      else: {:error, :disallowed_intents}
  end

  @doc "Intent bit required to receive `event` in a guild or direct context; 0 when ungated."
  @spec required_for(String.t(), scope()) :: non_neg_integer()
  def required_for(event, :guild), do: Map.get(@guild_requirements, event, 0)
  def required_for(event, :direct), do: Map.get(@direct_requirements, event, 0)

  @doc "Whether a session with `intents` receives `event` in `scope`."
  @spec allows?(non_neg_integer(), String.t(), scope()) :: boolean()
  def allows?(intents, event, scope) do
    required = required_for(event, scope)
    (intents &&& required) == required
  end

  @doc "Whether `intents` contains the named intent."
  @spec has?(non_neg_integer(), atom()) :: boolean()
  def has?(intents, name), do: (intents &&& Contracts.intent(name)) != 0
end
