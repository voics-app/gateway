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

  alias DiscordyGateway.Application.{Dispatch, DmCall, Guild, Ids, Pipeline, Ports, Settings}
  alias DiscordyGateway.Application.Presence.{MobileBackground, Publisher}
  alias DiscordyGateway.Application.Relay.{E2eeRelay, P2pRelay, TelemetryRelay}
  alias DiscordyGateway.Application.Session.{Authenticate, Identity, Ready, VoiceRouting}
  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Domain.Events.EventEnvelope
  alias DiscordyGateway.Domain.Intents
  alias DiscordyGateway.Domain.Protocol.Codec
  alias DiscordyGateway.Domain.Session.{Heartbeat, PassiveUpdates, RateLimiter, ResumeReplay}
  alias DiscordyGateway.Domain.Subscriber

  require Logger

  @presence_refresh_ms 30_000
  @passive_interval_ms 5_000
  @rejoin_delay_ms 1_000
  @resume_call_timeout_ms 10_000
  # Inspecting another process makes the caller wait for it, so the socket backlog is read on every
  # 32nd frame, not on each.
  @backlog_check_every 32

  defstruct [
    :id,
    :transport,
    :transport_ref,
    :identity,
    :heartbeat,
    :limiter,
    :detach_token,
    :redirect,
    background_token: nil,
    background_timer: nil,
    background_online: false,
    manual_status: "online",
    presence: nil,
    status: :connected,
    seq: 0,
    read_state_versioned: false,
    passive: false,
    passive_used: false,
    passive_token: nil,
    passive_updates: %PassiveUpdates{},
    passive_sent: %PassiveUpdates{},
    refreshing_guilds: MapSet.new(),
    activation_token: nil,
    channel_guilds: %{},
    private_channel_ids: MapSet.new(),
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

  # Full sweeps are frequent so that the large frames a session builds once (READY, GUILD_CREATE)
  # do not stay referenced from its old heap, which would pin their binaries for as long as the
  # session lives.
  def start_link(transport),
    do: GenServer.start_link(__MODULE__, transport, spawn_opt: [fullsweep_after: 10])

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
        {:resume, _transport, user_id, _seq},
        _from,
        %{identity: %Identity{user_id: user_id}, passive_used: true} = state
      ),
      do: {:stop, :normal, {:error, :passive_requires_identify}, state}

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

  def handle_call({:replace_background_session, identity}, _from, state) do
    if MobileBackground.same_login?(state, identity) do
      if state.transport,
        do: send(state.transport, {:gw_close, Contracts.close_code(:unknown_error)})

      {:stop, :normal, :ok, state}
    else
      {:reply, :ok, state}
    end
  end

  # GUILD_CREATE is the one large frame of a connection (every channel and member of the guild).
  # Hibernating right after it compacts the heap it grew while the frame was built, so an idle
  # session holds only its small steady-state.
  @impl true
  def handle_info({:guild_snapshot, guild_id, token, encoded}, %{status: :identified} = state) do
    data = Jason.decode!(encoded)
    state = remember_guild_channels(state, guild_id, data["channels"])

    cond do
      state.passive ->
        {:noreply, capture_guild_snapshot(state, guild_id, data), :hibernate}

      MapSet.member?(state.refreshing_guilds, guild_id) and token != state.activation_token ->
        {:noreply, state}

      true ->
        remaining = MapSet.delete(state.refreshing_guilds, guild_id)
        token = if MapSet.size(remaining) == 0, do: nil, else: state.activation_token
        state = %{state | refreshing_guilds: remaining, activation_token: token}
        {:noreply, dispatch(state, "GUILD_CREATE", encoded), :hibernate}
    end
  end

  def handle_info(
        {:guild_dispatch, guild_id, event, encoded, gate},
        %{status: :identified} = state
      ) do
    cond do
      state.passive -> {:noreply, capture_guild_event(state, guild_id, event, encoded, gate)}
      MapSet.member?(state.refreshing_guilds, guild_id) -> {:noreply, state}
      true -> {:noreply, dispatch(state, event, encoded)}
    end
  end

  def handle_info(
        {:dispatch, "GUILD_MEMBERS_CHUNK", _encoded},
        %{status: :identified, passive: true} = state
      ),
      do: {:noreply, state}

  def handle_info({:dispatch, event, encoded}, %{status: :identified} = state),
    do: {:noreply, dispatch(state, event, encoded)}

  def handle_info({:passive_flush, token}, %{passive: true, passive_token: token} = state) do
    case flush_passive(state) do
      {:ok, state} -> {:noreply, schedule_passive(state)}
      :error -> close(state, :unknown_error)
    end
  end

  # SESSION_REVOKE is bus-only: it ends the connection of a login session the api revoked.
  def handle_info(
        {:user_event, %EventEnvelope{type: "SESSION_REVOKE", data: data}},
        %{status: :identified} = state
      ) do
    if revoked?(state.identity, data),
      do: close(state, :authentication_failed),
      else: {:noreply, state}
  end

  def handle_info(
        {:user_event, %EventEnvelope{type: "READ_STATE_UPDATE", data: data}},
        %{status: :identified, passive: true} = state
      ) do
    {:noreply, %{state | passive_updates: PassiveUpdates.read_state(state.passive_updates, data)}}
  end

  def handle_info(
        {:user_event, %EventEnvelope{type: "MESSAGE_ACK"}},
        %{status: :identified, passive: true} = state
      ),
      do: {:noreply, state}

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

  def handle_info({:join_rejected, guild_id}, state) do
    state =
      if state.passive or MapSet.member?(state.refreshing_guilds, guild_id),
        do: dispatch(state, "GUILD_DELETE", Dispatch.encode(%{"id" => guild_id})),
        else: state

    {:noreply, forget_guild(state, guild_id)}
  end

  def handle_info({:guild_gone, guild_id}, %{passive: true} = state) do
    state = dispatch(state, "GUILD_DELETE", Dispatch.encode(%{"id" => guild_id}))
    {:noreply, forget_guild(state, guild_id)}
  end

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

  def handle_info({:resume_expired, token}, %{detach_token: token} = state) when token != nil do
    if MobileBackground.retained?(state), do: {:noreply, state}, else: {:stop, :normal, state}
  end

  def handle_info({:background_presence_expired, token}, %{background_token: token} = state)
      when token != nil do
    if state.transport == nil,
      do: {:stop, :normal, state},
      else: {:noreply, MobileBackground.clear(state)}
  end

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
    Publisher.refresh(presence_context(state))
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

  defp flush_passive(state) do
    visible = fn guild_id, channels ->
      case Guild.visible_channels(guild_id, self(), channels) do
        {:ok, ids} -> ids
        :error -> throw(:permission_unavailable)
      end
    end

    updates =
      state.passive_updates
      |> PassiveUpdates.filter_visible(visible)
      |> PassiveUpdates.filter_read_states(
        state.channel_guilds,
        state.private_channel_ids,
        visible
      )

    changes = PassiveUpdates.changed(updates, state.passive_sent)
    payload = PassiveUpdates.payload(changes)

    state =
      if map_size(payload) == 0,
        do: state,
        else: dispatch(state, "PASSIVE_UPDATE_V2", Dispatch.encode(payload))

    {:ok,
     %{
       state
       | passive_updates: %PassiveUpdates{},
         passive_sent: PassiveUpdates.merge(state.passive_sent, changes)
     }}
  catch
    :permission_unavailable -> :error
  end

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

  defp command(:session_mode, %{passive: passive}, state), do: change_session_mode(state, passive)

  defp command(cmd, payload, state), do: {:noreply, identified_command(cmd, payload, state)}

  defp identified_command(:presence_update, presence, state) do
    current = session_presence(presence, state.identity)
    Publisher.update(presence_context(state), current)
    manual = if presence.afk and presence.status == "idle", do: "online", else: presence.status
    MobileBackground.update(%{state | presence: current, manual_status: manual}, presence)
  end

  defp identified_command(:voice_state_update, request, state) do
    session = %{pid: self(), id: state.id, identity: state.identity}
    %{state | voice_scope: VoiceRouting.route(session, state.voice_scope, request)}
  end

  defp identified_command(:request_guild_members, _request, %{passive: true} = state), do: state

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

  defp identified_command(:call_ring, %{channel_id: channel_id, user_ids: user_ids}, state) do
    _ = DmCall.ring(channel_id, state.identity.user_id, user_ids)
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
      hydrate_read_states(state, identity, payload, ready)
    else
      {:error, :authentication_failed} -> close(state, :authentication_failed)
      {:error, :invalid_intents} -> close(state, :invalid_intents)
      {:error, :disallowed_intents} -> close(state, :disallowed_intents)
      {:error, :unauthorized} -> close(state, :authentication_failed)
      {:error, _reason} -> close(state, :unknown_error)
    end
  end

  defp hydrate_read_states(state, identity, payload, ready) do
    # Subscription precedes the snapshot. This GenServer cannot process queued
    # events until READY is sent, and channel versions resolve snapshot races.
    :ok = Ports.session_directory().register(state.id, identity.user_id, identity.device_id)

    case Ports.messages_client().fetch_read_states(identity.user_id) do
      {:ok, %{versioned: false}} when payload.passive ->
        close(state, :unknown_error)

      {:ok, reads} ->
        ready =
          Map.merge(ready, %{
            "read_state" => reads.entries,
            "read_state_versioned" => reads.versioned
          })

        state = %{state | read_state_versioned: reads.versioned}
        {:noreply, become_ready(state, identity, payload, ready)}

      {:error, _reason} ->
        Logger.warning("read-state hydration failed during identification")
        close(state, :unknown_error)
    end
  end

  defp become_ready(state, identity, payload, ready) do
    MobileBackground.replace_previous(identity)
    Process.send_after(self(), :refresh_presence, @presence_refresh_ms)

    state = %{
      state
      | identity: identity,
        status: :identified,
        friend_ids: Ready.friend_ids(ready),
        private_channel_ids:
          MapSet.new(for %{"id" => id} <- List.wrap(ready["private_channels"]), do: id),
        guild_ids: MapSet.new(Ready.guild_ids(ready))
    }

    :ok = Ports.call_defaults().put(identity.user_id, Ready.e2ee_calls_default?(ready))

    state =
      if payload.passive,
        do: schedule_passive(%{state | passive: true, passive_used: true}),
        else: state

    state = dispatch(state, "READY", Dispatch.encode(Ready.payload(identity, state.id, ready)))
    state = join_guilds(state, List.wrap(ready["guilds"]))
    replay_inbox(state)
    :ok = DmCall.announce_calls(identity.user_id, self())
    presence = session_presence(payload.presence, identity)
    Publisher.update(presence_context(state), presence)
    manual = get_in(ready, ["user_settings", "status"]) || "online"

    MobileBackground.update(
      %{state | presence: presence, manual_status: manual},
      payload.presence
    )
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

  defp schedule_passive(state) do
    token = make_ref()
    Process.send_after(self(), {:passive_flush, token}, @passive_interval_ms)
    %{state | passive_token: token}
  end

  defp change_session_mode(%{passive: passive} = state, passive), do: {:noreply, state}

  defp change_session_mode(%{read_state_versioned: false} = state, true),
    do: close(state, :unknown_error)

  defp change_session_mode(state, true) do
    {:noreply,
     schedule_passive(%{
       state
       | passive: true,
         passive_used: true,
         passive_sent: %PassiveUpdates{},
         refreshing_guilds: MapSet.new()
     })}
  end

  defp change_session_mode(state, false) do
    case resume_read_states(state) do
      {:ok, reads} ->
        guilds = Enum.map(state.guild_ids, &%{"id" => &1})

        state = %{
          state
          | passive: false,
            passive_token: nil,
            passive_updates: %PassiveUpdates{},
            refreshing_guilds: state.guild_ids,
            activation_token: make_ref()
        }

        state = sync_read_states(state, reads)

        case refresh_guilds(state, guilds) do
          {:ok, refreshed} -> {:noreply, refreshed}
          {:error, _reason} -> close(state, :unknown_error)
        end

      {:error, _} ->
        close(state, :unknown_error)
    end
  end

  defp remember_guild_channels(state, guild_id, channels) do
    channel_guilds =
      Enum.reduce(List.wrap(channels), state.channel_guilds, fn
        %{"id" => id}, acc -> Map.put(acc, id, guild_id)
        _, acc -> acc
      end)

    %{state | channel_guilds: channel_guilds}
  end

  defp capture_guild_snapshot(state, guild_id, data) do
    updates =
      Enum.reduce(List.wrap(data["channels"]), state.passive_updates, fn channel, acc ->
        case channel do
          %{"id" => channel_id, "last_message_id" => id} when is_binary(id) ->
            PassiveUpdates.message(acc, guild_id, %{"channel_id" => channel_id, "id" => id})

          _ ->
            acc
        end
      end)

    updates =
      Enum.reduce(List.wrap(data["voice_states"]), updates, fn voice, acc ->
        PassiveUpdates.voice_state(acc, voice, voice["channel_id"])
      end)

    %{state | passive_updates: updates}
  end

  defp capture_guild_event(state, guild_id, event, encoded, gate) do
    state =
      if event in ~w(CHANNEL_CREATE CHANNEL_UPDATE THREAD_CREATE THREAD_UPDATE) do
        remember_guild_channels(state, guild_id, [Jason.decode!(encoded)])
      else
        state
      end

    updates =
      case event do
        "MESSAGE_CREATE" ->
          PassiveUpdates.message(state.passive_updates, guild_id, Jason.decode!(encoded))

        "VOICE_STATE_UPDATE" ->
          PassiveUpdates.voice_state(state.passive_updates, Jason.decode!(encoded), gate)

        _ ->
          state.passive_updates
      end

    %{state | passive_updates: updates}
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
         {:ok, replay} <- ResumeReplay.select(buffer, seq, state.seq),
         {:ok, reads} <- resume_read_states(state) do
      state = state |> MobileBackground.clear() |> attach(transport)
      Enum.each(replay, &send(transport, {:gw_frame, &1.frame}))
      state = sync_read_states(state, reads)
      state = dispatch(state, "RESUMED", Dispatch.encode(%{}))
      replay_inbox(state)
      {:reply, :ok, state}
    else
      {:error, :invalid_seq} -> {:reply, {:error, :invalid_seq}, state}
      {:error, _gap_or_store} -> {:stop, :normal, {:error, :gap}, state}
    end
  end

  defp resume_read_states(state) do
    case Ports.messages_client().fetch_read_states(state.identity.user_id) do
      {:ok, %{versioned: false}} when state.read_state_versioned ->
        {:error, :read_state_authority_lost}

      result ->
        result
    end
  end

  defp sync_read_states(%{read_state_versioned: true} = state, %{
         versioned: true,
         entries: entries
       }) do
    dispatch(
      state,
      "READ_STATE_SYNC",
      Dispatch.encode(%{"read_state" => entries, "read_state_versioned" => true})
    )
  end

  defp sync_read_states(state, _reads), do: state

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
    Pipeline.measure(:session_dispatch, fn -> do_dispatch(state, event, encoded) end)
  end

  defp do_dispatch(state, event, encoded) do
    seq = state.seq + 1
    frame = seq |> Codec.dispatch_iodata(event, encoded) |> IO.iodata_to_binary()

    case Ports.resume_store().append(state.id, [{seq, frame}]) do
      :ok -> :ok
      {:error, reason} -> Logger.warning("resume buffer append failed: #{inspect(reason)}")
    end

    deliver_frame(state.transport, seq, frame)
    :telemetry.execute([:discordy_gateway, :dispatch, :sent], %{count: 1}, %{event: event})
    %{state | seq: seq}
  end

  # A socket that reads slower than frames arrive buffers them in its mailbox, without bound.
  # Past the backlog limit the socket process is killed instead: the frame is already in the resume
  # buffer, the session sees the transport go down and stays resumable, and the client reconnects
  # and resumes. One slow reader can then not grow gateway memory for everyone.
  defp deliver_frame(nil, _seq, _frame), do: :ok

  defp deliver_frame(transport, seq, frame) do
    if rem(seq, @backlog_check_every) == 0 and
         backlog(transport) > Settings.transport_backlog_limit() do
      :telemetry.execute([:discordy_gateway, :session, :slow_consumer], %{count: 1})
      Process.exit(transport, :kill)
    else
      send(transport, {:gw_frame, frame})
    end

    :ok
  end

  defp backlog(transport) when node(transport) == node() do
    case Process.info(transport, :message_queue_len) do
      {:message_queue_len, length} -> length
      nil -> 0
    end
  end

  defp backlog(_remote_transport), do: 0

  # GUILD_CREATE is dispatched by the guild process when this session joins it (filtered to the
  # channels the member can view, with voice states), so the raw event is only followed.
  defp user_event(state, %EventEnvelope{type: "GUILD_CREATE", data: data}),
    do: follow_membership(state, "GUILD_CREATE", data)

  defp user_event(state, %EventEnvelope{type: type, data: data}) do
    if Intents.allows?(state.identity.intents, type, :direct) do
      state = dispatch(state, type, Dispatch.encode(data))

      state
      |> follow_membership(type, data)
      |> follow_relationship(type, data)
      |> follow_settings(type, data)
      |> follow_private_channel(type, data)
    else
      state
    end
  end

  defp revoked?(%Identity{auth_session_id: id}, %{"session_ids" => ids})
       when is_binary(id) and is_list(ids),
       do: id in ids

  defp revoked?(_identity, _data), do: false

  defp follow_private_channel(state, "CHANNEL_CREATE", %{"id" => id} = channel) do
    if is_nil(channel["guild_id"]),
      do: %{state | private_channel_ids: MapSet.put(state.private_channel_ids, id)},
      else: state
  end

  defp follow_private_channel(state, "CHANNEL_DELETE", %{"id" => id}) do
    %{state | private_channel_ids: MapSet.delete(state.private_channel_ids, id)}
  end

  defp follow_private_channel(state, _type, _data), do: state

  defp follow_settings(state, "USER_SETTINGS_UPDATE", %{"status" => status} = data)
       when status in ~w(online idle dnd invisible) and status != state.manual_status do
    if MobileBackground.retained?(state) do
      state = MobileBackground.account_status(state, data)
      Publisher.update(presence_context(state), state.presence)
      state
    else
      %{state | manual_status: status}
    end
  end

  defp follow_settings(state, _type, _data), do: state

  defp follow_membership(state, "GUILD_CREATE", %{"id" => _} = guild) do
    %{state | guild_ids: MapSet.put(state.guild_ids, guild["id"])} |> join_guilds([guild])
  end

  defp follow_membership(state, "GUILD_DELETE", %{"id" => guild_id}),
    do: leave_guild(state, guild_id)

  defp follow_membership(state, _type, _data), do: state

  # A new friend's presence is not in READY: hand over their current presence and track them so
  # their later changes reach this session. Any other relationship (or a removal) stops tracking.
  defp follow_relationship(state, "RELATIONSHIP_ADD", relationship) when is_map(relationship) do
    id = Ready.relationship_user_id(relationship)

    cond do
      not is_binary(id) -> state
      Ready.friend?(relationship) -> befriend(state, id)
      true -> unfriend(state, id)
    end
  end

  defp follow_relationship(state, "RELATIONSHIP_REMOVE", %{"id" => id}) when is_binary(id),
    do: unfriend(state, id)

  defp follow_relationship(state, _type, _data), do: state

  defp befriend(state, id) do
    state = %{state | friend_ids: Enum.uniq([id | state.friend_ids])}

    Enum.reduce(Publisher.friend_payloads([id]), state, fn payload, acc ->
      dispatch(acc, "PRESENCE_UPDATE", Dispatch.encode(payload))
    end)
  end

  defp unfriend(state, id), do: %{state | friend_ids: List.delete(state.friend_ids, id)}

  ## Guild subscriptions

  defp join_guilds(state, guilds) do
    Enum.reduce(guilds, state, fn guild, acc -> join_guild(acc, guild["id"], seed(guild)) end)
  end

  defp seed(%{"roles" => _} = guild), do: guild
  defp seed(_guild), do: nil

  defp join_guild(state, guild_id, seed) do
    case subscribe_guild(state, guild_id, seed) do
      {:ok, joined} ->
        joined

      {:error, reason} ->
        Logger.warning("guild #{guild_id} unavailable: #{inspect(reason)}")
        state
    end
  end

  defp refresh_guilds(state, guilds) do
    Enum.reduce_while(guilds, {:ok, state}, fn guild, {:ok, acc} ->
      case subscribe_guild(acc, guild["id"], nil) do
        {:ok, joined} -> {:cont, {:ok, joined}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp subscribe_guild(state, guild_id, seed) do
    case Guild.join(guild_id, seed, subscriber(state)) do
      {:ok, pid} ->
        state = drop_guild_monitor(state, guild_id)
        ref = Process.monitor(pid)

        {:ok,
         %{
           state
           | guild_ids: MapSet.put(state.guild_ids, guild_id),
             guild_refs: Map.put(state.guild_refs, ref, guild_id)
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp subscriber(state) do
    %Subscriber{
      pid: self(),
      user_id: state.identity.user_id,
      intents: state.identity.intents,
      session_id: state.id,
      snapshot_token: state.activation_token,
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

    %{
      state
      | guild_ids: MapSet.delete(state.guild_ids, guild_id),
        refreshing_guilds: MapSet.delete(state.refreshing_guilds, guild_id)
    }
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
