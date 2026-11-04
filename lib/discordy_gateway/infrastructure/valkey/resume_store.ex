defmodule DiscordyGateway.Infrastructure.Valkey.ResumeStore do
  @moduledoc """
  Resume buffer in a Valkey list `gw:resume:<session_id>` (P7): capped at 1000 entries, each
  entry `<<seq::64, frame::binary>>`. The list expires on its own a few minutes after the last
  dispatch, which comfortably outlives the 120 s resume window.

  Appends are batched and asynchronous (`ResumeWriter`); `fetch/1` and `delete/1` first wait for
  the session's pending entries, so they see every entry appended before the call.
  """

  @behaviour DiscordyGateway.Application.Ports.ResumeStore

  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Infrastructure.Valkey.{Pool, ResumeWriter}

  @impl true
  def append(_session_id, []), do: :ok
  def append(session_id, entries), do: ResumeWriter.append(session_id, entries)

  @impl true
  def fetch(session_id) do
    :ok = ResumeWriter.flush(session_id)

    with {:ok, values} <- Pool.command(["LRANGE", Contracts.resume_key(session_id), 0, -1]) do
      {:ok, for(<<seq::64, frame::binary>> <- values, do: {seq, frame})}
    end
  end

  @impl true
  def delete(session_id) do
    :ok = ResumeWriter.flush(session_id)
    with {:ok, _count} <- Pool.command(["DEL", Contracts.resume_key(session_id)]), do: :ok
  end
end
