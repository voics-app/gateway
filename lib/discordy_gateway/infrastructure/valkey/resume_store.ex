defmodule DiscordyGateway.Infrastructure.Valkey.ResumeStore do
  @moduledoc """
  Resume buffer in a Valkey list `gw:resume:<session_id>` (P7): capped at 1000 entries, each
  entry `<<seq::64, frame::binary>>`. The list expires on its own a few minutes after the last
  dispatch, which comfortably outlives the 120 s resume window.
  """

  @behaviour DiscordyGateway.Application.Ports.ResumeStore

  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Infrastructure.Valkey.Pool

  @ttl_seconds 300

  @impl true
  def append(_session_id, []), do: :ok

  def append(session_id, entries) do
    key = Contracts.resume_key(session_id)
    values = for {seq, frame} <- entries, do: <<seq::64, frame::binary>>

    case Pool.pipeline([
           ["RPUSH", key | values],
           ["LTRIM", key, -Contracts.resume_buffer_cap(), -1],
           ["EXPIRE", key, @ttl_seconds]
         ]) do
      {:ok, _results} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def fetch(session_id) do
    with {:ok, values} <- Pool.command(["LRANGE", Contracts.resume_key(session_id), 0, -1]) do
      {:ok, for(<<seq::64, frame::binary>> <- values, do: {seq, frame})}
    end
  end

  @impl true
  def delete(session_id) do
    with {:ok, _count} <- Pool.command(["DEL", Contracts.resume_key(session_id)]), do: :ok
  end
end
