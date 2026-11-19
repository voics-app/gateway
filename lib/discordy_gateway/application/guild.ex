defmodule DiscordyGateway.Application.Guild do
  @moduledoc """
  One process per guild, registered cluster-wide through the `ServerDirectory` port and started
  lazily from `/guilds/{id}/state` (or from the READY payload that triggered the first join).

  It keeps the guild cache current from `discordy.guild.v1` events, fans events out to the
  subscribed sessions filtered by membership, intents and VIEW_CHANNEL (`Domain.Fanout`), owns
  the guild's voice states and voice node allocation, and answers REQUEST_GUILD_MEMBERS.
  """

  use GenServer, restart: :temporary

  alias DiscordyGateway.Application.{Dispatch, Pipeline, Ports, Settings}
  alias DiscordyGateway.Application.Voice.{Effects, Room}
  alias DiscordyGateway.Domain.Events.EventEnvelope
  alias DiscordyGateway.Domain.{Fanout, GuildState, Intents, Permissions, Presence, Subscriber}
  alias DiscordyGateway.Domain.Voice.{VoiceStates, VoiceTokenClaims}

  @create_member_cap 1_000
  @chunk_size 1_000
  @remove_events ~w(CHANNEL_DELETE THREAD_DELETE GUILD_MEMBER_REMOVE GUILD_ROLE_DELETE GUILD_DELETE)

  ## Client API

  @doc "Child spec for `ServerDirectory.ensure/2`. `seed` is an optional GuildFull payload."
  @spec spec(String.t(), map() | nil) :: Supervisor.child_spec()
  def spec(guild_id, seed) do
    name = Ports.server_directory().via({:guild, guild_id})

    %{
      id: {:guild, guild_id},
      start: {__MODULE__, :start_link, [[id: guild_id, seed: seed, name: name]]},
      restart: :temporary
    }
  end

  @doc "Starts the guild process if needed and subscribes the session to it."
  @spec join(String.t(), map() | nil, Subscriber.t()) :: {:ok, pid()} | {:error, term()}
  def join(guild_id, seed, %Subscriber{} = subscriber) do
    with {:ok, pid} <- Ports.server_directory().ensure({:guild, guild_id}, spec(guild_id, seed)) do
      GenServer.cast(pid, {:join, subscriber})
      {:ok, pid}
    end
  end

  @spec leave(String.t(), pid()) :: :ok
  def leave(guild_id, session_pid), do: cast(guild_id, {:leave, session_pid})

  @doc "Delivers a `discordy.guild.v1` or `discordy.message.v1` envelope to a running guild."
  @spec event(String.t(), EventEnvelope.t()) :: :ok | :not_running
  def event(guild_id, %EventEnvelope{} = envelope),
    do: cast(guild_id, {:event, envelope, System.monotonic_time()})

  @spec presence(String.t(), map()) :: :ok | :not_running
  def presence(guild_id, payload), do: cast(guild_id, {:presence, payload})

  @spec voice_state_update(String.t(), pid(), map()) :: :ok | :not_running
  def voice_state_update(guild_id, session_pid, request),
    do: cast(guild_id, {:voice_state_update, session_pid, request})

  @spec watch_together(String.t(), pid(), map()) :: :ok | :not_running
  def watch_together(guild_id, session_pid, command),
    do: cast(guild_id, {:watch_together, session_pid, command})

  @spec request_members(String.t(), pid(), map()) :: :ok | :not_running
  def request_members(guild_id, session_pid, request),
    do: cast(guild_id, {:request_members, session_pid, request})

  @doc "Revalidates channel visibility for a passive batch at delivery time."
  @spec visible_channels(String.t(), pid(), [String.t()]) :: {:ok, [String.t()]} | :error
  def visible_channels(guild_id, session_pid, channel_ids) do
    case Ports.server_directory().lookup({:guild, guild_id}) do
      {:ok, pid} ->
        {:ok, GenServer.call(pid, {:visible_channels, session_pid, channel_ids}, 1_000)}

      :error ->
        :error
    end
  catch
    :exit, _ -> :error
  end

  defp cast(guild_id, message) do
    case Ports.server_directory().lookup({:guild, guild_id}) do
      {:ok, pid} -> GenServer.cast(pid, message)
      :error -> :not_running
    end
  end

  ## Server

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

  @impl true
  def init(opts) do
    :ok = :pg.join(DiscordyGateway.Pg, :voice_servers, self())
    id = Keyword.fetch!(opts, :id)

    {:ok, %{id: id, seed: opts[:seed], cache: nil, subs: %{}, room: Room.new(id, id), idle: nil},
     {:continue, :load}}
  end

  @impl true
  def handle_continue(:load, %{seed: seed, id: id} = state) do
    case load(id, seed) do
      {:ok, cache} -> {:noreply, schedule_idle(%{state | cache: cache, seed: nil})}
      {:error, reason} -> {:stop, {:shutdown, reason}, state}
    end
  end

  @impl true
  def handle_call({:visible_channels, pid, channel_ids}, _from, state) do
    visible =
      case Map.fetch(state.subs, pid) do
        {:ok, %{sub: sub}} ->
          current = visible_channel_ids(state.cache, sub.user_id, System.os_time(:millisecond))
          Enum.filter(channel_ids, &MapSet.member?(current, &1))

        :error ->
          []
      end

    {:reply, visible, state}
  end

  @impl true
  def handle_cast({:join, %Subscriber{} = sub}, state),
    do: {:noreply, join_subscriber(state, sub)}

  def handle_cast({:leave, pid}, state), do: {:noreply, drop_subscriber(state, pid)}

  def handle_cast({:event, envelope, queued_at}, state) do
    Pipeline.since(:guild_queue, queued_at)
    handle_event(envelope, state)
  end

  def handle_cast({:presence, payload}, state) do
    fanout(state, "PRESENCE_UPDATE", Map.put(payload, "guild_id", state.id), nil, state.cache)
    {:noreply, state}
  end

  def handle_cast({:voice_state_update, pid, request}, state) do
    case Map.fetch(state.subs, pid) do
      {:ok, %{sub: sub}} -> {:noreply, voice_update(state, sub, request)}
      :error -> {:noreply, state}
    end
  end

  def handle_cast({:watch_together, pid, command}, state) do
    with {:ok, %{sub: sub}} <- Map.fetch(state.subs, pid),
         {room, effects} <-
           Room.watch(state.room, sub.user_id, command, System.os_time(:millisecond)) do
      Effects.run(effects, &broadcast_voice_state(state, &1, &2))
      {:noreply, %{state | room: room}}
    else
      _ -> {:noreply, state}
    end
  end

  def handle_cast({:request_members, pid, request}, state) do
    case Map.fetch(state.subs, pid) do
      {:ok, %{sub: sub}} -> send_member_chunks(state, sub, request)
      :error -> :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state),
    do: {:noreply, drop_subscriber(state, pid)}

  def handle_info(:idle_check, %{subs: subs} = state) when map_size(subs) == 0,
    do: {:stop, :normal, state}

  def handle_info(:idle_check, state), do: {:noreply, state}

  def handle_info({:voice_node_gone, node_id}, state) do
    {room, effects} =
      Room.node_gone(state.room, node_id, fn id -> Ports.session_directory().whereis(id) end)

    Effects.run(effects, &broadcast_voice_state(state, &1, &2))
    {:noreply, %{state | room: room}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  ## Loading

  # The api is the source of truth: events published before this process registered were routed
  # to no one, so a seed (the GUILD_CREATE snapshot) may already be stale. It is only a fallback.
  defp load(id, seed) do
    case Ports.api_client().fetch_guild_state(id) do
      {:ok, payload} -> {:ok, GuildState.from_payload(payload)}
      {:error, _reason} when is_map(seed) -> {:ok, GuildState.from_payload(seed)}
      {:error, reason} -> {:error, reason}
    end
  end

  ## Subscribers

  defp join_subscriber(state, %Subscriber{} = sub) do
    if GuildState.member?(state.cache, sub.user_id) do
      state = state |> drop_monitor(sub.pid) |> cancel_idle()
      ref = Process.monitor(sub.pid)
      encoded = Dispatch.encode(guild_create_payload(state, sub))
      send(sub.pid, {:guild_snapshot, state.id, sub.snapshot_token, encoded})
      %{state | subs: Map.put(state.subs, sub.pid, %{sub: sub, ref: ref})}
    else
      send(sub.pid, {:join_rejected, state.id})
      state
    end
  end

  defp drop_subscriber(state, pid) do
    case Map.fetch(state.subs, pid) do
      {:ok, %{sub: sub, ref: ref}} ->
        Process.demonitor(ref, [:flush])
        state = %{state | subs: Map.delete(state.subs, pid)}
        state = remove_voice_session(state, sub.session_id)
        schedule_idle(state)

      :error ->
        state
    end
  end

  defp drop_monitor(state, pid) do
    case Map.fetch(state.subs, pid) do
      {:ok, %{ref: ref}} -> Process.demonitor(ref, [:flush])
      :error -> :ok
    end

    state
  end

  defp schedule_idle(%{subs: subs} = state) when map_size(subs) == 0 do
    state = cancel_idle(state)
    %{state | idle: Process.send_after(self(), :idle_check, Settings.guild_idle_ms())}
  end

  defp schedule_idle(state), do: state

  defp cancel_idle(%{idle: nil} = state), do: state

  defp cancel_idle(%{idle: timer} = state) do
    Process.cancel_timer(timer)
    %{state | idle: nil}
  end

  defp subscribers(state), do: Enum.map(state.subs, fn {_pid, %{sub: sub}} -> sub end)

  ## GUILD_CREATE

  defp guild_create_payload(state, %Subscriber{} = sub) do
    cache = state.cache
    now = System.os_time(:millisecond)
    visible = visible_channel_ids(cache, sub.user_id, now)

    snapshot = GuildState.snapshot(cache)
    members = snapshot["members"]

    snapshot
    |> Map.put(
      "channels",
      Enum.filter(snapshot["channels"], &(&1["id"] in visible or thread_visible?(&1, visible)))
    )
    |> Map.put("members", Enum.take(members, @create_member_cap))
    |> Map.put("large", length(members) > @create_member_cap)
    |> Map.put("voice_states", voice_state_payloads(state, visible))
    |> Map.put("presences", presence_payloads(cache, sub))
  end

  defp visible_channel_ids(cache, user_id, now) do
    cache.channels
    |> Map.keys()
    |> Enum.filter(
      &Permissions.has?(GuildState.channel_permissions(cache, user_id, &1, now), :view_channel)
    )
    |> MapSet.new()
  end

  defp thread_visible?(%{"parent_id" => parent}, visible) when is_binary(parent),
    do: parent in visible

  defp thread_visible?(_channel, _visible), do: false

  defp voice_state_payloads(state, visible) do
    state.room
    |> Room.voice_states()
    |> Enum.filter(&(&1.channel_id in visible))
    |> Enum.map(&with_member(state, VoiceStates.to_payload(&1, state.id)))
  end

  defp presence_payloads(cache, sub) do
    if Intents.has?(sub.intents, :guild_presences) do
      cache
      |> GuildState.member_ids()
      |> Enum.take(@create_member_cap)
      |> Ports.presence_store().get_aggregates()
      |> Enum.map(fn {user_id, aggregate} ->
        Presence.dispatch_payload(user_id, aggregate, cache.id)
      end)
    else
      []
    end
  end

  ## Events

  # Bus-only (api#24): a moderator moved or disconnected a member. Never fanned out to clients.
  defp handle_event(%EventEnvelope{type: "VOICE_MEMBER_MOVE", data: data}, state),
    do: {:noreply, move_member(state, data["user_id"], data["channel_id"])}

  # Bus-only (api#52): asks the messages service to purge a banned user's recent messages. The
  # clients learn about it through MESSAGE_DELETE_BULK; this event is never fanned out.
  defp handle_event(%EventEnvelope{type: "BAN_MESSAGE_PURGE"}, state), do: {:noreply, state}

  defp handle_event(%EventEnvelope{type: type, data: data} = envelope, state) do
    updated = GuildState.apply_event(state.cache, type, data)
    plan_cache = if type in @remove_events, do: state.cache, else: updated
    fanout(state, type, data, envelope.channel_id, plan_cache)
    after_event(type, data, %{state | cache: updated})
  end

  defp after_event("GUILD_DELETE", _data, state) do
    Enum.each(state.subs, fn {pid, _} -> send(pid, {:guild_gone, state.id}) end)
    {:stop, :normal, state}
  end

  defp after_event("GUILD_MEMBER_REMOVE", data, state) do
    user_id = get_in(data, ["user", "id"])

    state =
      state.subs
      |> Enum.filter(fn {_pid, %{sub: sub}} -> sub.user_id == user_id end)
      |> Enum.reduce(state, fn {pid, _}, acc ->
        Dispatch.to_sessions([pid], "GUILD_DELETE", %{"id" => state.id})
        send(pid, {:guild_gone, state.id})
        drop_subscriber(acc, pid)
      end)

    {:noreply, state}
  end

  defp after_event(_type, _data, state), do: {:noreply, state}

  defp fanout(state, event, data, channel_id, plan_cache) do
    {plan, quiet} =
      Pipeline.measure(:fanout_plan, fn ->
        plan =
          Fanout.plan(
            plan_cache,
            subscribers(state),
            event,
            data,
            channel_id,
            System.os_time(:millisecond)
          )

        {plan, quiet_pids(state, plan, event, data)}
      end)

    Pipeline.measure(:fanout_send, fn ->
      plan
      |> Enum.group_by(
        fn {pid, variant} -> {variant, MapSet.member?(quiet, pid)} end,
        &elem(&1, 0)
      )
      |> Enum.each(fn {{variant, quiet?}, pids} ->
        Dispatch.to_guild_sessions(
          pids,
          state.id,
          event,
          variant_payload(data, variant, quiet?),
          channel_id
        )
      end)
    end)
  end

  defp variant_payload(data, variant, quiet?) do
    data = if variant == :stripped, do: Fanout.strip_content(data), else: data
    if quiet?, do: Fanout.quiet_here(data), else: data
  end

  # An @here message pings online members only: sessions of users whose aggregated presence is
  # offline (or unknown) still get the message, without the broadcast flags.
  defp quiet_pids(state, plan, event, data) do
    if Fanout.here_only?(event, data) and plan != [] do
      users = Map.new(plan, fn {pid, _variant} -> {pid, state.subs[pid].sub.user_id} end)
      aggregates = users |> Map.values() |> Enum.uniq() |> Ports.presence_store().get_aggregates()

      for {pid, user_id} <- users,
          not Fanout.here_audience?(aggregates[user_id]),
          into: MapSet.new(),
          do: pid
    else
      MapSet.new()
    end
  end

  ## Voice

  defp voice_update(state, sub, %{channel_id: nil} = request),
    do: apply_voice(state, sub, request, nil)

  defp voice_update(state, sub, %{channel_id: channel_id} = request) do
    member = member(sub)
    now = System.os_time(:millisecond)
    permissions = GuildState.channel_permissions(state.cache, sub.user_id, channel_id, now)

    if GuildState.voice_channel?(state.cache, channel_id) and
         Permissions.has?(permissions, :connect) do
      apply_voice(state, sub, request, join_opts(state, channel_id))
    else
      Dispatch.to_sessions(
        [sub.pid],
        "VOICE_STATE_UPDATE",
        with_member(state, Room.rejected_payload(state.room, member, request))
      )

      state
    end
  end

  defp join_opts(state, channel_id) do
    %{
      region:
        GuildState.voice_region_for(state.cache, channel_id, Settings.default_voice_region()),
      e2ee: VoiceTokenClaims.e2ee_for(GuildState.channel(state.cache, channel_id)),
      whereis: fn id -> Ports.session_directory().whereis(id) end
    }
  end

  # The api already checked the moderator's permissions, so the member's own CONNECT is not
  # re-checked (as in Discord). A user who is not in voice, or a target that is not a voice
  # channel of this guild, is ignored. A move re-uses the Room join path, which broadcasts the new
  # state and sends the member a VOICE_SERVER_UPDATE (token for the new channel); a disconnect
  # broadcasts channel_id null, which the member's client treats as leaving voice.
  defp move_member(state, user_id, target) do
    with %{} = current <- VoiceStates.get(state.room.states, user_id),
         {:ok, sub} <- subscriber_by_session(state, current.session_id),
         true <- target == nil or GuildState.voice_channel?(state.cache, target),
         true <- target != current.channel_id do
      request = %{
        guild_id: state.id,
        channel_id: target,
        self_mute: current.self_mute,
        self_deaf: current.self_deaf,
        self_video: current.self_video,
        self_stream: current.self_stream
      }

      opts = if target, do: join_opts(state, target), else: nil
      apply_voice(state, sub, request, opts)
    else
      _ -> state
    end
  end

  defp subscriber_by_session(state, session_id) do
    state.subs
    |> Map.values()
    |> Enum.find_value(:error, fn %{sub: sub} ->
      if sub.session_id == session_id, do: {:ok, sub}
    end)
  end

  defp apply_voice(state, sub, request, opts) do
    {room, effects} = Room.update(state.room, member(sub), request, opts)
    Effects.run(effects, &broadcast_voice_state(state, &1, &2))
    %{state | room: room}
  end

  defp remove_voice_session(state, nil), do: state

  defp remove_voice_session(state, session_id) do
    {room, effects} = Room.remove_session(state.room, session_id)
    Effects.run(effects, &broadcast_voice_state(state, &1, &2))
    %{state | room: room}
  end

  defp broadcast_voice_state(state, payload, gate_channel_id),
    do:
      fanout(
        state,
        "VOICE_STATE_UPDATE",
        with_member(state, payload),
        gate_channel_id,
        state.cache
      )

  defp with_member(state, %{"user_id" => user_id} = payload) do
    case Map.fetch(state.cache.members, user_id) do
      {:ok, member} -> Map.put(payload, "member", member)
      :error -> payload
    end
  end

  defp member(%Subscriber{} = sub),
    do: %{pid: sub.pid, user_id: sub.user_id, session_id: sub.session_id}

  ## REQUEST_GUILD_MEMBERS

  defp send_member_chunks(state, sub, request) do
    if sub.bot == false or Intents.has?(sub.intents, :guild_members) do
      {members, not_found} = select_members(state.cache, request)
      chunks = members |> Enum.chunk_every(@chunk_size) |> ensure_one_chunk()
      count = length(chunks)

      chunks
      |> Enum.with_index()
      |> Enum.each(fn {chunk, index} ->
        payload = chunk_payload(state, request, chunk, index, count, not_found)
        Dispatch.to_sessions([sub.pid], "GUILD_MEMBERS_CHUNK", payload)
      end)
    end
  end

  defp select_members(cache, %{user_ids: ids}) when is_list(ids),
    do: GuildState.members_by_ids(cache, ids)

  defp select_members(cache, %{query: query, limit: limit}) do
    capped = if limit == 0, do: @chunk_size * 10, else: limit
    {GuildState.search_members(cache, query || "", capped), []}
  end

  defp ensure_one_chunk([]), do: [[]]
  defp ensure_one_chunk(chunks), do: chunks

  defp chunk_payload(state, request, members, index, count, not_found) do
    base = %{
      "guild_id" => state.id,
      "members" => members,
      "chunk_index" => index,
      "chunk_count" => count
    }

    base
    |> put_if(not_found != [], "not_found", not_found)
    |> put_if(request.nonce != nil, "nonce", request.nonce)
    |> put_if(request.presences, "presences", member_presences(state, members))
  end

  defp member_presences(state, members) do
    ids = Enum.map(members, &get_in(&1, ["user", "id"]))

    ids
    |> Ports.presence_store().get_aggregates()
    |> Enum.map(fn {user_id, aggregate} ->
      Presence.dispatch_payload(user_id, aggregate, state.id)
    end)
  end

  defp put_if(map, true, key, value), do: Map.put(map, key, value)
  defp put_if(map, false, _key, _value), do: map
end
