defmodule DiscordyGateway.Application.DmCall do
  @moduledoc """
  One process per DM channel with an active call, registered cluster-wide through the
  `ServerDirectory` port. It plays the guild's voice role for DMs: voice states, the shared voice
  node allocation (P8) and Watch Together. Voice state changes go to every session of the
  channel's recipients; the process stops when the last participant leaves.
  """

  use GenServer, restart: :temporary

  alias DiscordyGateway.Application.{Dispatch, Ports, Settings}
  alias DiscordyGateway.Application.Voice.{Effects, Room}
  alias DiscordyGateway.Domain.Voice.VoiceTokenClaims

  @type access :: %{String.t() => term()}

  ## Client API

  @spec spec(String.t()) :: Supervisor.child_spec()
  def spec(channel_id) do
    name = Ports.server_directory().via({:dm_call, channel_id})

    %{
      id: {:dm_call, channel_id},
      start: {__MODULE__, :start_link, [[id: channel_id, name: name]]},
      restart: :temporary
    }
  end

  @doc "Applies a session's voice state request in the call of `channel_id`."
  @spec voice_state_update(String.t(), map(), map(), access() | nil) :: :ok | {:error, term()}
  def voice_state_update(channel_id, member, request, access) do
    with {:ok, pid} <- Ports.server_directory().ensure({:dm_call, channel_id}, spec(channel_id)) do
      GenServer.cast(pid, {:voice_state_update, member, request, access})
    end
  end

  @doc "Leaves the call if it is running; never starts one."
  @spec leave(String.t(), map(), map()) :: :ok | :not_running
  def leave(channel_id, member, request) do
    case Ports.server_directory().lookup({:dm_call, channel_id}) do
      {:ok, pid} -> GenServer.cast(pid, {:voice_state_update, member, request, nil})
      :error -> :not_running
    end
  end

  @spec watch_together(String.t(), String.t(), map()) :: :ok | :not_running
  def watch_together(channel_id, user_id, command) do
    case Ports.server_directory().lookup({:dm_call, channel_id}) do
      {:ok, pid} -> GenServer.cast(pid, {:watch_together, user_id, command})
      :error -> :not_running
    end
  end

  ## Server

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

  @impl true
  def init(opts) do
    id = Keyword.fetch!(opts, :id)
    :ok = :pg.join(DiscordyGateway.Pg, :voice_servers, self())

    {:ok,
     %{id: id, room: Room.new(id, nil), monitors: %{}, recipients: [], delivery_mode: "standard"}}
  end

  @impl true
  def handle_cast({:voice_state_update, member, request, access}, state) do
    state = remember_access(state, access)
    state = if request.channel_id, do: monitor(state, member), else: state

    opts = %{
      region: Settings.default_voice_region(),
      e2ee: VoiceTokenClaims.e2ee_for(%{"delivery_mode" => state.delivery_mode}),
      whereis: fn id -> Ports.session_directory().whereis(id) end
    }

    {room, effects} = Room.update(state.room, member, request, opts)
    state = %{state | room: room}
    Effects.run(effects, &broadcast(state, &1, &2))
    stop_if_empty(state)
  end

  def handle_cast({:watch_together, user_id, command}, state) do
    case Room.watch(state.room, user_id, command, System.os_time(:millisecond)) do
      {room, effects} ->
        Effects.run(effects, &broadcast(state, &1, &2))
        {:noreply, %{state | room: room}}

      :error ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    case Map.pop(state.monitors, pid) do
      {nil, _monitors} ->
        {:noreply, state}

      {session_id, monitors} ->
        {room, effects} = Room.remove_session(state.room, session_id)
        state = %{state | room: room, monitors: monitors}
        Effects.run(effects, &broadcast(state, &1, &2))
        stop_if_empty(state)
    end
  end

  def handle_info({:voice_node_gone, node_id}, state) do
    {room, effects} =
      Room.node_gone(state.room, node_id, fn id -> Ports.session_directory().whereis(id) end)

    Effects.run(effects, &broadcast(state, &1, &2))
    {:noreply, %{state | room: room}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp remember_access(state, nil), do: state

  defp remember_access(state, access) do
    %{
      state
      | recipients: List.wrap(access["recipient_ids"]),
        delivery_mode: access["delivery_mode"] || state.delivery_mode
    }
  end

  defp monitor(state, %{pid: pid, session_id: session_id}) do
    if Map.has_key?(state.monitors, pid) do
      state
    else
      Process.monitor(pid)
      %{state | monitors: Map.put(state.monitors, pid, session_id)}
    end
  end

  defp broadcast(state, payload, _gate_channel_id) do
    participants = Enum.map(Room.voice_states(state.room), & &1.user_id)
    users = Enum.uniq(state.recipients ++ participants ++ [payload["user_id"]])
    pids = Enum.flat_map(users, fn id -> Ports.session_directory().sessions_for_user(id) end)
    Dispatch.to_sessions(Enum.uniq(pids), "VOICE_STATE_UPDATE", payload)
  end

  defp stop_if_empty(state) do
    if Room.empty?(state.room), do: {:stop, :normal, state}, else: {:noreply, state}
  end
end
