defmodule DiscordyGateway.Application.Session do
  @moduledoc """
  One process per gateway connection (P7). The WebSocket process is a thin transport: it forwards
  decoded frames here and writes back whatever this process sends it, so the session survives a
  dropped connection for the resume window and can be re-attached by RESUME, possibly from a
  gateway node other than the one holding the process.

  Lifecycle: HELLO, IDENTIFY (JWT or bot introspection), READY from the api, guild
  subscriptions, heartbeat supervision, send rate limiting, RESUME from the Valkey buffer.

  Messages sent to the transport: `{:gw_frame, binary | iodata}` and `{:gw_close, code}`.
  """

  use GenServer, restart: :temporary

  alias DiscordyGateway.Application.{Dispatch, DmCall, Guild, Ids, Ports, Settings}
  alias DiscordyGateway.Application.Presence.Publisher
  alias DiscordyGateway.Application.Relay.{E2eeRelay, P2pRelay, TelemetryRelay}
  alias DiscordyGateway.Application.Session.{Authenticate, Identity, Ready, VoiceRouting}
  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Domain.Events.EventEnvelope
  alias DiscordyGateway.Domain.Intents
  alias DiscordyGateway.Domain.Protocol.Codec
  alias DiscordyGateway.Domain.Session.{Heartbeat, RateLimiter, ResumeReplay}
  alias DiscordyGateway.Domain.Subscriber

  require Logger

  @presence_refresh_ms 30_000
  @rejoin_delay_ms 1_000
  @resume_call_timeout_ms 10_000

  defstruct [
    :id,
    :transport,
    :transport_ref,
    :identity,
    :heartbeat,
    :limiter,
    :detach_token,
    :redirect,
    status: :connected,
    seq: 0,
    guild_ids: MapSet.new(),
    guild_refs: %{},
    friend_ids: [],
    voice_scope: nil,
    relay_worker: nil
  ]

  ## Client API

  @doc "Starts a session for a freshly accepted connection whose process is `transport`."
  @spec start(pid()) :: {:ok, pid()} | {:error, term()}
  def start(transport),
    do: DynamicSupervisor.start_child(DiscordyGateway.SessionSupervisor, {__MODULE__, transport})

  def start_link(transport), do: GenServer.start_link(__MODULE__, transport)

  @doc "Hands a decoded client frame to the session."
  @spec frame(pid(), term()) :: :ok
  def frame(session, frame), do: GenServer.cast(session, {:frame, frame})

  ## Server

  @impl true
  def init(transport) do
    Process.flag(:trap_exit, true)
    interval = Settings.heartbeat_interval_ms()

    state = %__MODULE__{
      id: Ids.uuid_v4(),
      transport: transport,
      transport_ref: Process.monitor(transport),
      heartbeat: Heartbeat.new(interval, now()),
      limiter: RateLimiter.p7()
    }

    send_frame(state, Codec.hello(interval))
    Process.send_after(self(), :identify_timeout, Settings.identify_timeout_ms())
    Process.send_after(self(), :check_heartbeat, Heartbeat.check_interval(state.heartbeat))
    telemetry(:started)
    {:ok, state}
  end

  @impl true
  def handle_cast({:frame, frame}, %{redirect: pid} = state) when is_pid(pid) do
    GenServer.cast(pid, {:frame, frame})
    {:noreply, state}
  end

  def handle_cast({:frame, frame}, state) do
    case RateLimiter.hit(state.limiter, now()) do
      {:ok, limiter} -> decode(frame, %{state | limiter: limiter})
      :rate_limited -> close(state, :rate_limited)
    end
  end

  @impl true
  def handle_call(
        {:resume, transport, user_id, seq},
        _from,
        %{identity: %Identity{user_id: user_id}} = state
      ),
      do: resume_attach(transport, seq, state)

  def handle_call({:resume, _transport, _user_id, _seq}, _from, state),
    do: {:reply, {:error, :mismatch}, state}

  def handle_call(
        {:deliver_p2p, user_id, payload},
        _from,
        %{identity: %Identity{user_id: user_id}, transport: t} = state
      )
      when is_pid(t) do
    send_frame(state, Codec.relay(:p2p_signal, payload))
    {:reply, :ok, state}
  end

  def handle_call({:deliver_p2p, _user_id, _payload}, _from, state), do: {:reply, :error, state}

  @impl true
  def handle_info({:dispatch, event, encoded}, %{status: :identified} = state),
    do: {:noreply, dispatch(state, event, encoded)}

  def handle_info({:user_event, %EventEnvelope{} = envelope}, %{status: :identified} = state),
    do: {:noreply, user_event(state, envelope)}

  def handle_info({:relay, kind, payload}, %{status: :identified} = state) do
    send_frame(state, Codec.relay(kind, payload))
    {:noreply, state}
  end

  def handle_info(
        {:relay_to_user, user_id, kind, payload},
        %{identity: %Identity{user_id: user_id}} = state
      ) do
    send_frame(state, Codec.relay(kind, payload))
    {:noreply, state}
  end

  def handle_info({:join_guild, guild_id}, %{status: :identified} = state),
    do: {:noreply, join_guilds(state, [%{"id" => guild_id}])}

  def handle_info({:join_rejected, guild_id}, state),
    do: {:noreply, forget_guild(state, guild_id)}

  def handle_info({:guild_gone, guild_id}, state), do: {:noreply, forget_guild(state, guild_id)}

  def handle_info({:rejoin_guild, guild_id}, %{status: :identified} = state) do
    if MapSet.member?(state.guild_ids, guild_id),
      do: {:noreply, join_guilds(state, [%{"id" => guild_id}])},
      else: {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{transport_ref: ref} = state),
    do: transport_down(state)

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.guild_refs, ref) do
      {nil, _refs} -> {:noreply, state}
      {guild_id, refs} -> {:noreply, guild_down(%{state | guild_refs: refs}, guild_id, reason)}
    end
  end

  def handle_info(:identify_timeout, %{status: :connected, redirect: nil} = state),
    do: close(state, :not_authenticated)

  def handle_info(:check_heartbeat, state) do
    Process.send_after(self(), :check_heartbeat, Heartbeat.check_interval(state.heartbeat))

    if state.transport != nil and state.redirect == nil and
         Heartbeat.expired?(state.heartbeat, now()) do
      close(state, :session_timed_out)
    else
      {:noreply, state}
    end
  end

  def handle_info({:resume_expired, token}, %{detach_token: token} = state) when token != nil,
    do: {:stop, :normal, state}

  def handle_info({:voice_detach_expired, token}, %{detach_token: token} = state)
      when token != nil and state.voice_scope != nil do
    session = %{pid: self(), id: state.id, identity: state.identity}

    {:noreply,
     %{
       state
       | voice_scope:
           VoiceRouting.route(session, state.voice_scope, leave_request(state.voice_scope))
     }}
  end

  def handle_info(:refresh_presence, %{status: :identified} = state) do
    Ports.presence_store().refresh(state.identity.user_id)
    Process.send_after(self(), :refresh_presence, @presence_refresh_ms)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{status: :identified} = state) do
    Ports.resume_store().delete(state.id)
    Publisher.drop(presence_context(state))
    telemetry(:stopped)
  end

  def terminate(_reason, _state), do: :ok

  ## Inbound frames

  defp decode(frame, state) do
    case Codec.decode_inbound(frame) do
      {:ok, {command, payload}} -> command(command, payload, state)
      {:error, code} -> close_with(state, code)
    end
  end

  defp command(:heartbeat, seq, state) when is_integer(seq) and seq > state.seq,
    do: close(state, :invalid_seq)

  defp command(:heartbeat, _seq, state) do
    send_frame(state, Codec.heartbeat_ack())
    {:noreply, %{state | heartbeat: Heartbeat.beat(state.heartbeat, now())}}
  end

  defp command(:identify, payload, %{status: :connected} = state), do: identify(payload, state)
  defp command(:resume, payload, %{status: :connected} = state), do: resume(payload, state)

  defp command(cmd, _payload, %{status: :identified} = state) when cmd in [:identify, :resume],
    do: close(state, :already_authenticated)

  defp command(_cmd, _payload, %{status: :connected} = state),
    do: close(state, :not_authenticated)

  defp command(cmd, payload, state), do: {:noreply, identified_command(cmd, payload, state)}

  defp identified_command(:presence_update, presence, state) do
    Publisher.update(presence_context(state), session_presence(presence, state.identity))
    state
  end

  defp identified_command(:voice_state_update, request, state) do
    session = %{pid: self(), id: state.id, identity: state.identity}
    %{state | voice_scope: VoiceRouting.route(session, state.voice_scope, request)}
  end

  defp identified_command(:request_guild_members, request, state) do
    _ = Guild.request_members(request.guild_id, self(), request)
    state
  end

  defp identified_command(:e2ee_envelope, envelope, state),
    do: in_order(state, fn pid -> E2eeRelay.relay(state.identity, pid, envelope) end)

  defp identified_command(:e2ee_ack, %{message_ids: ids}, state),
    do: async(state, fn _pid -> E2eeRelay.ack(state.identity, ids) end)

  defp identified_command(:p2p_signal, signal, state),
    do: in_order(state, fn pid -> P2pRelay.relay(state.identity, pid, signal) end)

  defp identified_command(:voice_telemetry, sample, %{voice_scope: scope} = state)
       when scope != nil,
       do:
         async(state, fn _pid ->
           TelemetryRelay.publish(state.identity.user_id, state.id, sample)
         end)

  defp identified_command(:voice_telemetry, _sample, state), do: state

  defp identified_command(:watch_together, command, %{voice_scope: {:guild, guild_id}} = state) do
    _ = Guild.watch_together(guild_id, self(), command)
    state
  end

  defp identified_command(
         :watch_together,
         command,
         %{voice_scope: {:dm_call, channel_id}} = state
       ) do
    _ = DmCall.watch_together(channel_id, state.identity.user_id, command)
    state
  end

  defp identified_command(:watch_together, _command, state), do: state

  defp identified_command(:call_decline, %{channel_id: channel_id}, state) do
    _ = DmCall.decline(channel_id, state.identity.user_id)
    state
  end

  # E2EE envelopes and P2P signals must reach recipients in the order the client sent them (a
  # welcome before the messages of its group, an offer before its ICE candidates), so they run one
  # after another on a worker owned by this session instead of on a task each.
  defp in_order(state, fun) do
    worker = state.relay_worker || start_relay_worker(self())
    send(worker, {:run, fun})
    %{state | relay_worker: worker}
  end

  defp start_relay_worker(session) do
    {:ok, pid} =
      Task.Supervisor.start_child(DiscordyGateway.TaskSupervisor, fn ->
        ref = Process.monitor(session)
        relay_loop(session, ref)
      end)

    pid
  end

  defp relay_loop(session, ref) do
    receive do
      {:run, fun} ->
        try do
          fun.(session)
        rescue
          error -> Logger.error("relay failed: #{Exception.message(error)}")
        end

        relay_loop(session, ref)

      {:DOWN, ^ref, :process, _pid, _reason} ->
        :ok
    end
  end

  defp async(state, fun) do
    session = self()

    {:ok, _pid} =
      Task.Supervisor.start_child(DiscordyGateway.TaskSupervisor, fn -> fun.(session) end)

    state
  end

  ## IDENTIFY

  defp identify(payload, state) do
    with {:ok, identity} <- Authenticate.identify(payload),
         {:ok, ready} <- Ports.api_client().fetch_ready(identity.user_id) do
      {:noreply, become_ready(state, identity, payload, ready)}
    else
      {:error, :authentication_failed} -> close(state, :authentication_failed)
      {:error, :invalid_intents} -> close(state, :invalid_intents)
      {:error, :disallowed_intents} -> close(state, :disallowed_intents)
      {:error, :unauthorized} -> close(state, :authentication_failed)
      {:error, _reason} -> close(state, :unknown_error)
    end
  end

  defp become_ready(state, identity, payload, ready) do
    :ok = Ports.session_directory().register(state.id, identity.user_id, identity.device_id)
    Process.send_after(self(), :refresh_presence, @presence_refresh_ms)

    state = %{
      state
      | identity: identity,
        status: :identified,
        friend_ids: Ready.friend_ids(ready),
        guild_ids: MapSet.new(Ready.guild_ids(ready))
    }

    :ok = Ports.call_defaults().put(identity.user_id, Ready.e2ee_calls_default?(ready))
    state = dispatch(state, "READY", Dispatch.encode(Ready.payload(identity, state.id, ready)))
    state = join_guilds(state, List.wrap(ready["guilds"]))
    replay_inbox(state)
    :ok = DmCall.announce_calls(identity.user_id, self())
    Publisher.update(presence_context(state), session_presence(payload.presence, identity))
    state
  end

  defp session_presence(nil, identity),
    do: session_presence(%{status: "online", activities: [], afk: false}, identity)

  defp session_presence(presence, identity), do: Map.put(presence, :platform, identity.platform)

  defp presence_context(state) do
    %{
      user_id: state.identity.user_id,
      session_id: state.id,
      guild_ids: MapSet.to_list(state.guild_ids),
      friend_ids: state.friend_ids
    }
  end

  defp replay_inbox(state) do
    for payload <- E2eeRelay.pending(state.identity),
        do: send_frame(state, Codec.relay(:e2ee_envelope, payload))

    :ok
  end

  ## RESUME

  defp resume(payload, state) do
    with {:ok, auth} <- Authenticate.authenticate_token(payload.token),
         pid when is_pid(pid) <- Ports.session_directory().whereis(payload.session_id),
         true <- pid != self() do
      resume_remote(pid, auth.user_id, payload.seq, state)
    else
      {:error, :authentication_failed} -> close(state, :authentication_failed)
      _unknown_session -> invalid_session(state)
    end
  end

  defp resume_remote(pid, user_id, seq, state) do
    case GenServer.call(pid, {:resume, state.transport, user_id, seq}, @resume_call_timeout_ms) do
      :ok -> {:noreply, %{state | redirect: pid, status: :forwarding}}
      {:error, :invalid_seq} -> close(state, :invalid_seq)
      {:error, _reason} -> invalid_session(state)
    end
  catch
    :exit, _reason -> invalid_session(state)
  end

  defp invalid_session(state) do
    send_frame(state, Codec.invalid_session(false))
    {:noreply, state}
  end

  defp resume_attach(transport, seq, state) do
    with {:ok, entries} <- Ports.resume_store().fetch(state.id),
         buffer = Enum.map(entries, fn {entry_seq, frame} -> %{seq: entry_seq, frame: frame} end),
         {:ok, replay} <- ResumeReplay.select(buffer, seq, state.seq) do
      state = attach(state, transport)
      Enum.each(replay, &send(transport, {:gw_frame, &1.frame}))
      state = dispatch(state, "RESUMED", Dispatch.encode(%{}))
      replay_inbox(state)
      {:reply, :ok, state}
    else
      {:error, :invalid_seq} -> {:reply, {:error, :invalid_seq}, state}
      {:error, _gap_or_store} -> {:stop, :normal, {:error, :gap}, state}
    end
  end

  defp attach(state, transport) do
    if state.transport_ref, do: Process.demonitor(state.transport_ref, [:flush])

    if state.transport && state.transport != transport,
      do: send(state.transport, {:gw_close, Contracts.close_code(:unknown_error)})

    %{
      state
      | transport: transport,
        transport_ref: Process.monitor(transport),
        detach_token: nil,
        heartbeat: Heartbeat.beat(state.heartbeat, now())
    }
  end

  ## Dispatch and events

  defp dispatch(state, event, encoded) do
    seq = state.seq + 1
    frame = seq |> Codec.dispatch_iodata(event, encoded) |> IO.iodata_to_binary()

    case Ports.resume_store().append(state.id, [{seq, frame}]) do
      :ok -> :ok
      {:error, reason} -> Logger.warning("resume buffer append failed: #{inspect(reason)}")
    end

    if state.transport, do: send(state.transport, {:gw_frame, frame})
    :telemetry.execute([:discordy_gateway, :dispatch, :sent], %{count: 1}, %{event: event})
    %{state | seq: seq}
  end

  # GUILD_CREATE is dispatched by the guild process when this session joins it (filtered to the
  # channels the member can view, with voice states), so the raw event is only followed.
  defp user_event(state, %EventEnvelope{type: "GUILD_CREATE", data: data}),
    do: follow_membership(state, "GUILD_CREATE", data)

  defp user_event(state, %EventEnvelope{type: type, data: data}) do
    if Intents.allows?(state.identity.intents, type, :direct) do
      state = dispatch(state, type, Dispatch.encode(data))
      follow_membership(state, type, data)
    else
      state
    end
  end

  defp follow_membership(state, "GUILD_CREATE", %{"id" => _} = guild) do
    %{state | guild_ids: MapSet.put(state.guild_ids, guild["id"])} |> join_guilds([guild])
  end

  defp follow_membership(state, "GUILD_DELETE", %{"id" => guild_id}),
    do: leave_guild(state, guild_id)

  defp follow_membership(state, _type, _data), do: state

  ## Guild subscriptions

  defp join_guilds(state, guilds) do
    Enum.reduce(guilds, state, fn guild, acc -> join_guild(acc, guild["id"], seed(guild)) end)
  end

  defp seed(%{"roles" => _} = guild), do: guild
  defp seed(_guild), do: nil

  defp join_guild(state, guild_id, seed) do
    case Guild.join(guild_id, seed, subscriber(state)) do
      {:ok, pid} ->
        state = drop_guild_monitor(state, guild_id)
        ref = Process.monitor(pid)

        %{
          state
          | guild_ids: MapSet.put(state.guild_ids, guild_id),
            guild_refs: Map.put(state.guild_refs, ref, guild_id)
        }

      {:error, reason} ->
        Logger.warning("guild #{guild_id} unavailable: #{inspect(reason)}")
        state
    end
  end

  defp subscriber(state) do
    %Subscriber{
      pid: self(),
      user_id: state.identity.user_id,
      intents: state.identity.intents,
      session_id: state.id,
      bot: Identity.bot?(state.identity),
      application_id: state.identity.application_id
    }
  end

  defp leave_guild(state, guild_id) do
    _ = Guild.leave(guild_id, self())
    forget_guild(state, guild_id)
  end

  defp forget_guild(state, guild_id) do
    state = drop_guild_monitor(state, guild_id)
    %{state | guild_ids: MapSet.delete(state.guild_ids, guild_id)}
  end

  defp drop_guild_monitor(state, guild_id) do
    {matching, others} = Enum.split_with(state.guild_refs, fn {_ref, id} -> id == guild_id end)
    Enum.each(matching, fn {ref, _id} -> Process.demonitor(ref, [:flush]) end)
    %{state | guild_refs: Map.new(others)}
  end

  defp guild_down(state, guild_id, reason)
       when reason in [:normal, :shutdown] or (is_tuple(reason) and elem(reason, 0) == :shutdown),
       do: forget_guild(state, guild_id)

  defp guild_down(state, guild_id, _reason) do
    Process.send_after(self(), {:rejoin_guild, guild_id}, @rejoin_delay_ms)
    state
  end

  ## Connection state

  defp transport_down(%{status: :identified} = state) do
    token = make_ref()
    Process.send_after(self(), {:resume_expired, token}, Settings.resume_window_ms())

    if state.voice_scope,
      do:
        Process.send_after(
          self(),
          {:voice_detach_expired, token},
          Settings.voice_detach_grace_ms()
        )

    {:noreply, %{state | transport: nil, transport_ref: nil, detach_token: token}}
  end

  defp transport_down(state), do: {:stop, :normal, state}

  defp leave_request(scope) do
    %{
      guild_id: scope_guild(scope),
      channel_id: nil,
      self_mute: false,
      self_deaf: false,
      self_video: false,
      self_stream: false
    }
  end

  defp scope_guild({:guild, guild_id}), do: guild_id
  defp scope_guild(_scope), do: nil

  defp close(state, reason), do: close_with(state, Contracts.close_code(reason))

  @resumable_closes [4008, 4009]

  defp close_with(%{status: :identified} = state, code) when code in @resumable_closes do
    if state.transport, do: send(state.transport, {:gw_close, code})
    telemetry(:closed, code)
    {:noreply, detach(state)}
  end

  defp close_with(state, code) do
    if state.transport, do: send(state.transport, {:gw_close, code})
    telemetry(:closed, code)
    {:stop, :normal, state}
  end

  defp detach(state) do
    Process.demonitor(state.transport_ref, [:flush])
    {:noreply, state} = transport_down(%{state | transport_ref: nil})
    state
  end

  defp send_frame(%{transport: nil}, _frame), do: :ok

  defp send_frame(%{transport: transport}, frame),
    do: send(transport, {:gw_frame, Jason.encode_to_iodata!(frame)})

  defp now, do: System.monotonic_time(:millisecond)

  defp telemetry(event, code \\ nil),
    do: :telemetry.execute([:discordy_gateway, :session, event], %{count: 1}, %{code: code})
end
