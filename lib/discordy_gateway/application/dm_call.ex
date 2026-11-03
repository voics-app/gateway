defmodule DiscordyGateway.Application.DmCall do
  @moduledoc """
  One process per DM channel with an active call, registered cluster-wide through the
  `ServerDirectory` port. It plays the guild's voice role for DMs: voice states, the shared voice
  node allocation (P8), Watch Together and call ringing. Voice state changes go to every session
  of the channel's recipients; the process stops when the last participant leaves.

  Ringing: the first join creates the call (CALL_CREATE to the recipients) and rings every other
  recipient. A ringing user stops ringing when they join, decline (op 25) or after the ring
  timeout. A participant can ring further recipients later (op 26). Later changes send CALL_UPDATE;
  the last leave sends CALL_DELETE.
  """

  use GenServer, restart: :temporary

  alias DiscordyGateway.Application.{Dispatch, Ports, Settings}
  alias DiscordyGateway.Application.Voice.{Effects, Room}
  alias DiscordyGateway.Domain.Voice.{VoiceStates, VoiceTokenClaims}

  @type access :: %{String.t() => term()}

  @pg DiscordyGateway.Pg
  @default_ring_timeout_ms 30_000

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

  @doc "The user declines the ringing call of `channel_id`; a no-op when they are not ringing."
  @spec decline(String.t(), String.t()) :: :ok
  def decline(channel_id, user_id) do
    case Ports.server_directory().lookup({:dm_call, channel_id}) do
      {:ok, pid} -> GenServer.cast(pid, {:decline, user_id})
      :error -> :ok
    end
  end

  @doc """
  A participant rings more recipients of the running call of `channel_id`. Users who are not
  recipients or are already in the call are ignored; a no-op when the caller is not in the call
  or no call runs.
  """
  @spec ring(String.t(), String.t(), [String.t()]) :: :ok
  def ring(channel_id, caller_id, user_ids) do
    case Ports.server_directory().lookup({:dm_call, channel_id}) do
      {:ok, pid} -> GenServer.cast(pid, {:ring, caller_id, user_ids})
      :error -> :ok
    end
  end

  @doc "Sends the active calls `user_id` rings for or takes part in to `session_pid` (after READY)."
  @spec announce_calls(String.t(), pid()) :: :ok
  def announce_calls(user_id, session_pid) do
    for pid <- :pg.get_members(@pg, {:call_user, user_id}),
        do: GenServer.cast(pid, {:announce, user_id, session_pid})

    :ok
  end

  ## Server

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

  @impl true
  def init(opts) do
    id = Keyword.fetch!(opts, :id)
    :ok = :pg.join(@pg, :voice_servers, self())

    {:ok,
     %{
       id: id,
       room: Room.new(id, nil),
       monitors: %{},
       recipients: [],
       delivery_mode: "standard",
       ringing: %{},
       members: MapSet.new()
     }}
  end

  @impl true
  def handle_cast({:voice_state_update, member, request, access}, state) do
    before = call_of(state)
    state = remember_access(state, access)
    state = if request.channel_id, do: monitor(state, member), else: state

    opts = %{
      region: Settings.default_voice_region(),
      e2ee: VoiceTokenClaims.e2ee_for(%{"delivery_mode" => state.delivery_mode}),
      whereis: fn id -> Ports.session_directory().whereis(id) end
    }

    {room, effects} = Room.update(state.room, member, request, opts)
    state = %{state | room: room}
    state = if before == nil and not Room.empty?(room), do: ring(state), else: state
    finish(state, before, effects)
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

  def handle_cast({:decline, user_id}, state) do
    before = call_of(state)
    finish(%{state | ringing: drop_ringer(state.ringing, user_id)}, before, [])
  end

  def handle_cast({:ring, caller_id, user_ids}, state) do
    if caller_id in participant_ids(state) do
      before = call_of(state)

      targets =
        Enum.filter(user_ids, &(&1 in state.recipients and &1 not in participant_ids(state)))

      finish(ring(state, targets), before, [])
    else
      {:noreply, state}
    end
  end

  def handle_cast({:announce, user_id, session_pid}, state) do
    if Room.user_in_channel?(state.room, user_id, state.id) or
         Map.has_key?(state.ringing, user_id),
       do: Dispatch.to_sessions([session_pid], "CALL_CREATE", call_of(state))

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    case Map.pop(state.monitors, pid) do
      {nil, _monitors} ->
        {:noreply, state}

      {session_id, monitors} ->
        before = call_of(state)
        {room, effects} = Room.remove_session(state.room, session_id)
        finish(%{state | room: room, monitors: monitors}, before, effects)
    end
  end

  def handle_info({:ring_timeout, user_id}, state) do
    before = call_of(state)
    finish(%{state | ringing: drop_ringer(state.ringing, user_id)}, before, [])
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

  # Settles ringing and the membership of the :pg groups, then sends the call lifecycle event
  # (CALL_CREATE, CALL_UPDATE or CALL_DELETE) comparing the call before and after the change.
  defp finish(state, before, effects) do
    state = settle_ringing(state)
    state = sync_members(state)
    Effects.run(effects, &broadcast(state, &1, &2))

    for {event, payload} <- call_events(before, call_of(state), state.id),
        do: Dispatch.to_sessions(audience(state, []), event, payload)

    stop_if_empty(state)
  end

  defp broadcast(state, payload, _gate_channel_id) do
    Dispatch.to_sessions(audience(state, [payload["user_id"]]), "VOICE_STATE_UPDATE", payload)
  end

  # Recipients, participants and the extra users: every session of each is entitled to the event.
  defp audience(state, extra) do
    participants = Enum.map(Room.voice_states(state.room), & &1.user_id)

    directory = Ports.session_directory()

    (state.recipients ++ participants ++ extra)
    |> Enum.uniq()
    |> Enum.flat_map(&directory.sessions_for_user/1)
    |> Enum.uniq()
  end

  defp ring(state), do: ring(state, state.recipients -- participant_ids(state))

  # Starts (or restarts) the ring timeout of every target.
  defp ring(state, targets) do
    timeout = Application.get_env(:discordy_gateway, :ring_timeout_ms, @default_ring_timeout_ms)

    ringing =
      Enum.reduce(targets, state.ringing, fn user_id, ringing ->
        timer = Process.send_after(self(), {:ring_timeout, user_id}, timeout)
        ringing |> drop_ringer(user_id) |> Map.put(user_id, timer)
      end)

    %{state | ringing: ringing}
  end

  # Ringing only lasts while the call has participants, and stops for anyone who has joined.
  defp settle_ringing(state) do
    if Room.empty?(state.room) do
      cancel_all(state.ringing)
      %{state | ringing: %{}}
    else
      {joined, ringing} = Map.split(state.ringing, participant_ids(state))
      cancel_all(joined)
      %{state | ringing: ringing}
    end
  end

  defp drop_ringer(ringing, user_id) do
    case Map.pop(ringing, user_id) do
      {nil, rest} ->
        rest

      {timer, rest} ->
        Process.cancel_timer(timer)
        rest
    end
  end

  defp cancel_all(ringing),
    do: Enum.each(ringing, fn {_user_id, timer} -> Process.cancel_timer(timer) end)

  # The :pg group `{:call_user, id}` lists the calls a user rings for or takes part in.
  defp sync_members(state) do
    wanted = MapSet.new(Map.keys(state.ringing) ++ participant_ids(state))

    for user_id <- MapSet.difference(wanted, state.members),
        do: :pg.join(@pg, {:call_user, user_id}, self())

    for user_id <- MapSet.difference(state.members, wanted),
        do: :pg.leave(@pg, {:call_user, user_id}, self())

    %{state | members: wanted}
  end

  defp participant_ids(state), do: Enum.map(Room.voice_states(state.room), & &1.user_id)

  # Discord-shaped call object, or nil when no one is in the call.
  defp call_of(%{room: room} = state) do
    if Room.empty?(room) do
      nil
    else
      %{
        "channel_id" => state.id,
        "message_id" => nil,
        "region" => Settings.default_voice_region(),
        "ringing" => state.ringing |> Map.keys() |> Enum.sort(),
        "voice_states" =>
          room
          |> Room.voice_states()
          |> Enum.sort_by(& &1.user_id)
          |> Enum.map(&VoiceStates.to_payload(&1, nil))
      }
    end
  end

  defp call_events(nil, nil, _channel_id), do: []
  defp call_events(nil, call, _channel_id), do: [{"CALL_CREATE", call}]

  defp call_events(_before, nil, channel_id),
    do: [{"CALL_DELETE", %{"channel_id" => channel_id, "message_id" => nil}}]

  defp call_events(call, call, _channel_id), do: []
  defp call_events(_before, call, _channel_id), do: [{"CALL_UPDATE", call}]

  defp stop_if_empty(state) do
    if Room.empty?(state.room), do: {:stop, :normal, state}, else: {:noreply, state}
  end
end
