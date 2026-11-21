defmodule DiscordyGateway.Application.Dispatch do
  @moduledoc """
  Delivery of dispatch payloads to session processes. A payload is JSON-encoded once into a single
  binary and that binary is shared by every recipient; each session adds its own sequence number.

  The payload is a binary, not iodata: a message that carries a nested iodata list is copied onto
  the heap of every receiving process (a 200-member GUILD_CREATE is a list of thousands of small
  binaries, about 300 KiB per session), while a binary above 64 bytes is reference counted and
  sending it copies only a pointer.
  """

  @spec encode(term()) :: binary()
  def encode(payload), do: Jason.encode!(payload)

  @doc "Sends an already encoded payload to one session."
  @spec send_encoded(pid(), String.t(), binary()) :: :ok
  def send_encoded(pid, event, encoded) do
    send(pid, {:dispatch, event, encoded})
    :ok
  end

  @doc "Encodes `payload` once and sends it to every session in `pids`."
  @spec to_sessions([pid()], String.t(), term()) :: :ok
  def to_sessions([], _event, _payload), do: :ok

  def to_sessions(pids, event, payload) do
    encoded = encode(payload)
    Enum.each(pids, &send_encoded(&1, event, encoded))
  end

  @doc "Sends a guild event with its permission gate to subscribed sessions."
  @spec to_guild_sessions([pid()], String.t(), String.t(), term(), String.t() | nil) :: :ok
  def to_guild_sessions([], _guild_id, _event, _payload, _gate), do: :ok

  def to_guild_sessions(pids, guild_id, event, payload, gate) do
    encoded = encode(payload)
    Enum.each(pids, &send(&1, {:guild_dispatch, guild_id, event, encoded, gate}))
  end

  @doc "Sends a relay frame (op 20, 22 or 24) to a session."
  @spec relay(pid(), atom(), map()) :: :ok
  def relay(pid, kind, payload) do
    send(pid, {:relay, kind, payload})
    :ok
  end
end
