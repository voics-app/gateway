defmodule DiscordyGateway.Contracts do
  @moduledoc """
  Local stand-in for the generated `contracts` package (P6/P7/P8/P9 of the master plan).

  Everything that is pinned across services lives here and only here: opcodes, close codes,
  intent and permission bits, topic names, Valkey/etcd key layouts and protocol timings. It is
  pure data with no IO, so the domain may depend on it the same way it would depend on the
  generated package once that is published.
  """

  import Bitwise

  @opcodes %{
    dispatch: 0,
    heartbeat: 1,
    identify: 2,
    presence_update: 3,
    voice_state_update: 4,
    resume: 6,
    reconnect: 7,
    request_guild_members: 8,
    invalid_session: 9,
    hello: 10,
    heartbeat_ack: 11,
    e2ee_envelope: 20,
    e2ee_ack: 21,
    p2p_signal: 22,
    voice_telemetry: 23,
    watch_together: 24,
    call_decline: 25,
    call_ring: 26
  }

  @close_codes %{
    unknown_error: 4000,
    unknown_opcode: 4001,
    decode_error: 4002,
    not_authenticated: 4003,
    authentication_failed: 4004,
    already_authenticated: 4005,
    invalid_seq: 4007,
    rate_limited: 4008,
    session_timed_out: 4009,
    invalid_intents: 4013,
    disallowed_intents: 4014,
    server_overloaded: 4015
  }

  @intents %{
    guilds: 1 <<< 0,
    guild_members: 1 <<< 1,
    guild_moderation: 1 <<< 2,
    guild_emojis: 1 <<< 3,
    guild_integrations: 1 <<< 4,
    guild_webhooks: 1 <<< 5,
    guild_invites: 1 <<< 6,
    guild_voice_states: 1 <<< 7,
    guild_presences: 1 <<< 8,
    guild_messages: 1 <<< 9,
    guild_message_reactions: 1 <<< 10,
    guild_message_typing: 1 <<< 11,
    direct_messages: 1 <<< 12,
    direct_message_reactions: 1 <<< 13,
    direct_message_typing: 1 <<< 14,
    message_content: 1 <<< 15,
    guild_scheduled_events: 1 <<< 16,
    guild_message_polls: 1 <<< 24,
    direct_message_polls: 1 <<< 25
  }

  @permissions %{
    create_instant_invite: 1 <<< 0,
    kick_members: 1 <<< 1,
    ban_members: 1 <<< 2,
    administrator: 1 <<< 3,
    manage_channels: 1 <<< 4,
    manage_guild: 1 <<< 5,
    add_reactions: 1 <<< 6,
    view_audit_log: 1 <<< 7,
    priority_speaker: 1 <<< 8,
    stream: 1 <<< 9,
    view_channel: 1 <<< 10,
    send_messages: 1 <<< 11,
    send_tts_messages: 1 <<< 12,
    manage_messages: 1 <<< 13,
    embed_links: 1 <<< 14,
    attach_files: 1 <<< 15,
    read_message_history: 1 <<< 16,
    mention_everyone: 1 <<< 17,
    use_external_emojis: 1 <<< 18,
    view_guild_insights: 1 <<< 19,
    connect: 1 <<< 20,
    speak: 1 <<< 21,
    mute_members: 1 <<< 22,
    deafen_members: 1 <<< 23,
    move_members: 1 <<< 24,
    use_vad: 1 <<< 25,
    change_nickname: 1 <<< 26,
    manage_nicknames: 1 <<< 27,
    manage_roles: 1 <<< 28,
    manage_webhooks: 1 <<< 29,
    manage_guild_expressions: 1 <<< 30,
    use_application_commands: 1 <<< 31,
    request_to_speak: 1 <<< 32,
    manage_events: 1 <<< 33,
    manage_threads: 1 <<< 34,
    create_public_threads: 1 <<< 35,
    create_private_threads: 1 <<< 36,
    use_external_stickers: 1 <<< 37,
    send_messages_in_threads: 1 <<< 38,
    use_embedded_activities: 1 <<< 39,
    moderate_members: 1 <<< 40,
    view_creator_monetization_analytics: 1 <<< 41,
    use_soundboard: 1 <<< 42,
    create_guild_expressions: 1 <<< 43,
    create_events: 1 <<< 44,
    use_external_sounds: 1 <<< 45,
    send_voice_messages: 1 <<< 46,
    send_polls: 1 <<< 49,
    use_external_apps: 1 <<< 50
  }

  @topics %{
    guild: "discordy.guild.v1",
    message: "discordy.message.v1",
    user: "discordy.user.v1",
    interaction: "discordy.interaction.v1",
    telemetry: "discordy.telemetry.v1",
    audit: "discordy.audit.v1"
  }

  @doc "Numeric value of a P7 opcode."
  @spec opcode(atom()) :: non_neg_integer()
  def opcode(name), do: Map.fetch!(@opcodes, name)

  @doc "All P7 opcodes, name to value."
  @spec opcodes() :: %{atom() => non_neg_integer()}
  def opcodes, do: @opcodes

  @doc "Numeric value of a P7 close code."
  @spec close_code(atom()) :: pos_integer()
  def close_code(name), do: Map.fetch!(@close_codes, name)

  @doc "All P7 close codes, name to value."
  @spec close_codes() :: %{atom() => pos_integer()}
  def close_codes, do: @close_codes

  @doc "Bit value of a gateway intent."
  @spec intent(atom()) :: pos_integer()
  def intent(name), do: Map.fetch!(@intents, name)

  @doc "All gateway intents, name to bit."
  @spec intents() :: %{atom() => pos_integer()}
  def intents, do: @intents

  @doc "Bit value of a Discord permission."
  @spec permission(atom()) :: pos_integer()
  def permission(name), do: Map.fetch!(@permissions, name)

  @doc "All Discord permissions, name to bit."
  @spec permissions() :: %{atom() => pos_integer()}
  def permissions, do: @permissions

  @doc "Kafka topic name for a P6 topic."
  @spec topic(atom()) :: String.t()
  def topic(name), do: Map.fetch!(@topics, name)

  @doc "All P6 topics, name to topic string."
  @spec topics() :: %{atom() => String.t()}
  def topics, do: @topics

  @doc "Heartbeat interval announced in HELLO (P7)."
  def heartbeat_interval_ms, do: 41_250

  @doc "Resume window after a connection drops (P7)."
  def resume_window_ms, do: 120_000

  @doc "Maximum number of buffered dispatches per session (P7)."
  def resume_buffer_cap, do: 1_000

  @doc "Client send budget per window (P7)."
  def rate_limit_sends, do: 120

  @doc "Client send window (P7)."
  def rate_limit_window_ms, do: 60_000

  @doc "E2EE inbox stream approximate MAXLEN (P7)."
  def e2ee_inbox_maxlen, do: 10_000

  @doc "Hard TTL of an E2EE inbox entry (P7)."
  def e2ee_ttl_ms, do: 7 * 24 * 60 * 60 * 1000

  @doc "Lifetime of a voice token in seconds (P3)."
  def voice_token_ttl_s, do: 60

  @doc "Size of the per-node event dedup LRU (P6)."
  def dedup_capacity, do: 10_000

  @doc "Valkey resume list for a session (P7)."
  def resume_key(session_id), do: "gw:resume:" <> session_id

  @doc "Valkey E2EE inbox stream for a device (P7)."
  def e2ee_inbox_key(device_id), do: "e2ee:inbox:" <> device_id

  @doc "Valkey sorted set indexing E2EE entries by expiry (P7)."
  def e2ee_ttl_key, do: "e2ee:ttl"

  @doc "Valkey voice allocation for a guild or DM channel (P8)."
  def voice_alloc_key(server_id), do: "voice:alloc:" <> server_id

  @doc "etcd prefix where voice nodes register (P8)."
  def voice_nodes_prefix, do: "/discordy/voice/nodes/"

  @doc "Voice WebSocket path on a voice node (P8)."
  def voice_ws_path, do: "/voice"
end
