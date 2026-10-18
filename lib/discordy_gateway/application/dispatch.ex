defmodule DiscordyGateway.Application.Dispatch do
  @moduledoc """
  Delivery of dispatch payloads to session processes. A payload is JSON-encoded once and the
  resulting iodata is shared by every recipient; each session adds its own sequence number.
  """

  @spec encode(term()) :: iodata()
  def encode(payload), do: Jason.encode_to_iodata!(payload)

  @doc "Sends an already encoded payload to one session."
  @spec send_encoded(pid(), String.t(), iodata()) :: :ok
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

  @doc "Sends a relay frame (op 20, 22 or 24) to a session."
  @spec relay(pid(), atom(), map()) :: :ok
  def relay(pid, kind, payload) do
    send(pid, {:relay, kind, payload})
    :ok
  end
end
