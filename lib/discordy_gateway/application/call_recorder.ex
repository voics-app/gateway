defmodule DiscordyGateway.Application.CallRecorder do
  @moduledoc """
  Keeps the call system message (type 3) of one DM call in the messages service. Started by the
  `DmCall` when the call begins: it posts the message, records participants as they join and
  stamps the end when the call is over (or when the owning `DmCall` dies). Best effort: failures
  are logged and never reach the call, and the voice process never waits on HTTP because all
  requests run here, in order.

  The owner talks to it with `joined/2` and `finish/2`.
  """

  require Logger

  alias DiscordyGateway.Application.Ports

  @doc "Starts a recorder owned by the calling process; `participants` are in the call already."
  @spec start(String.t(), [String.t(), ...]) :: pid()
  def start(channel_id, [caller | _] = participants) do
    owner = self()

    {:ok, pid} =
      Task.Supervisor.start_child(DiscordyGateway.TaskSupervisor, fn ->
        run(owner, channel_id, caller, participants)
      end)

    pid
  end

  @doc "Everyone who took part in the call so far."
  @spec joined(pid(), [String.t()]) :: :ok
  def joined(pid, participants) do
    send(pid, {:participants, participants})
    :ok
  end

  @doc "The call is over; `participants` is everyone who took part."
  @spec finish(pid(), [String.t()]) :: :ok
  def finish(pid, participants) do
    send(pid, {:finish, participants})
    :ok
  end

  defp run(owner, channel_id, caller, participants) do
    monitor = Process.monitor(owner)

    case Ports.messages_client().create_call_message(channel_id, caller, participants) do
      {:ok, message_id} ->
        loop(%{channel_id: channel_id, message_id: message_id, monitor: monitor}, participants)

      {:error, reason} ->
        Logger.warning("call message of #{channel_id} not created: #{inspect(reason)}")
    end
  end

  defp loop(ctx, known) do
    receive do
      {:participants, ^known} ->
        loop(ctx, known)

      {:participants, participants} ->
        update(ctx, %{participants: participants})
        loop(ctx, participants)

      {:finish, participants} ->
        update(ctx, %{participants: participants, ended_timestamp: now()})

      {:DOWN, monitor, :process, _pid, _reason} when monitor == ctx.monitor ->
        update(ctx, %{participants: known, ended_timestamp: now()})
    end
  end

  defp update(ctx, body) do
    case Ports.messages_client().update_call_message(ctx.channel_id, ctx.message_id, body) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "call message #{ctx.message_id} of #{ctx.channel_id} not updated: #{inspect(reason)}"
        )
    end
  end

  defp now, do: DateTime.utc_now() |> DateTime.to_iso8601()
end
