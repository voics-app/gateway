defmodule DiscordyGateway.Domain.Protocol.Codec do
  @moduledoc """
  P7 frame codec. Inbound frames arrive as already-parsed JSON maps; they are validated and
  turned into `{command, payload}` tuples, or an error carrying the P7 close code to use.
  Outbound frames are built as plain maps, except DISPATCH, which is emitted as iodata around a
  payload that was JSON-encoded once for all recipients.
  """

  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Domain.Protocol.Payloads

  @type command ::
          :heartbeat
          | :identify
          | :presence_update
          | :voice_state_update
          | :resume
          | :request_guild_members
          | :e2ee_envelope
          | :e2ee_ack
          | :p2p_signal
          | :voice_telemetry
          | :watch_together
          | :call_decline
          | :call_ring

  @client_ops %{
    1 => :heartbeat,
    2 => :identify,
    3 => :presence_update,
    4 => :voice_state_update,
    6 => :resume,
    8 => :request_guild_members,
    20 => :e2ee_envelope,
    21 => :e2ee_ack,
    22 => :p2p_signal,
    23 => :voice_telemetry,
    24 => :watch_together,
    25 => :call_decline,
    26 => :call_ring
  }

  @doc "Validates an inbound frame map and decodes its payload."
  @spec decode_inbound(term()) :: {:ok, {command(), term()}} | {:error, pos_integer()}
  def decode_inbound(%{"op" => op} = frame) when is_integer(op) do
    case Map.fetch(@client_ops, op) do
      {:ok, command} -> decode_payload(command, Map.get(frame, "d"))
      :error -> {:error, Contracts.close_code(:unknown_opcode)}
    end
  end

  def decode_inbound(_frame), do: {:error, Contracts.close_code(:decode_error)}

  defp decode_payload(command, d) do
    case Payloads.decode(command, d) do
      {:ok, payload} -> {:ok, {command, payload}}
      {:error, :invalid_intents} -> {:error, Contracts.close_code(:invalid_intents)}
      {:error, _reason} -> {:error, Contracts.close_code(:decode_error)}
    end
  end

  @doc "HELLO frame (op 10)."
  def hello(interval_ms), do: frame(:hello, %{"heartbeat_interval" => interval_ms})

  @doc "HEARTBEAT_ACK frame (op 11)."
  def heartbeat_ack, do: frame(:heartbeat_ack, nil)

  @doc "Server-initiated HEARTBEAT request (op 1) carrying the last sequence."
  def heartbeat_request(seq), do: frame(:heartbeat, seq)

  @doc "RECONNECT frame (op 7)."
  def reconnect, do: frame(:reconnect, nil)

  @doc "INVALID_SESSION frame (op 9)."
  def invalid_session(resumable) when is_boolean(resumable),
    do: frame(:invalid_session, resumable)

  @doc "Relay frame for the non-dispatch server-to-client opcodes 20, 22 and 24."
  def relay(kind, d) when kind in [:e2ee_envelope, :p2p_signal, :watch_together],
    do: frame(kind, d)

  @doc "DISPATCH frame (op 0) as iodata wrapping an already-encoded JSON payload."
  @spec dispatch_iodata(non_neg_integer(), String.t(), iodata()) :: iodata()
  def dispatch_iodata(seq, event_name, encoded_payload) when is_integer(seq) do
    [
      ~s({"op":0,"s":),
      Integer.to_string(seq),
      ~s(,"t":"),
      event_name,
      ~s(","d":),
      encoded_payload,
      "}"
    ]
  end

  defp frame(name, d), do: %{"op" => Contracts.opcode(name), "d" => d, "s" => nil, "t" => nil}
end
