defmodule DiscordyGateway.Application.Voice.Room do
  @moduledoc """
  Voice state of one voice server scope, a guild or a DM call: who is connected, which node
  serves them (P8), and the Watch Together state. Operations return the new room plus a list of
  effects that the owning process (guild or DM call) delivers, because delivery differs between
  the two.

  Effects:
    * `{:broadcast_state, payload, gate_channel_id}` - VOICE_STATE_UPDATE for everyone entitled to see it
    * `{:server_update, pid, payload}` - VOICE_SERVER_UPDATE for one session
    * `{:rejected, pid, payload}` - the join failed; `payload` is the session's own left state
    * `{:join_failed, pid, payload}` - VOICE_JOIN_FAILED, the reason a join was refused (the
      participants' call settings could not be read under the `wait` policy)
    * `{:relay_watch, session_ids, payload}` - WATCH_TOGETHER relay frame (op 24) for sessions
    * `{:relay_watch_to_pid, pid, payload}` - the current Watch Together state for a late joiner
  """

  alias DiscordyGateway.Application.Ports
  alias DiscordyGateway.Application.Voice.{Allocator, E2eePolicy}
  alias DiscordyGateway.Domain.Relay.WatchTogether
  alias DiscordyGateway.Domain.Voice.{VoiceNode, VoiceStates, VoiceTokenClaims}

  @enforce_keys [:server_id]
  defstruct [:server_id, :guild_id, :node_id, :region, states: VoiceStates.new(), watch: %{}]

  @type t :: %__MODULE__{}
  @type effect :: tuple()
  @type member :: %{pid: pid(), user_id: String.t(), session_id: String.t()}
  # `e2ee` is the channel or delivery-mode requirement. `whereis` maps a session id to its pid, so
  # participants already in the call can be re-issued a token when the call becomes E2EE.
  @type join_opts :: %{
          region: String.t(),
          e2ee: boolean(),
          whereis: (String.t() -> pid() | nil)
        }

  @spec new(String.t(), String.t() | nil) :: t()
  def new(server_id, guild_id), do: %__MODULE__{server_id: server_id, guild_id: guild_id}

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{states: states}), do: VoiceStates.empty?(states)

  @spec voice_states(t()) :: [VoiceStates.state()]
  def voice_states(%__MODULE__{states: states}), do: VoiceStates.all(states)

  @spec user_in_channel?(t(), String.t(), String.t()) :: boolean()
  def user_in_channel?(%__MODULE__{states: states}, user_id, channel_id),
    do: match?(%{channel_id: ^channel_id}, VoiceStates.get(states, user_id))

  @doc "Applies a VOICE_STATE_UPDATE request. `join_opts` is only used when a node is needed."
  @spec update(t(), member(), map(), join_opts()) :: {t(), [effect()]}
  def update(room, member, request, join_opts) do
    previous = VoiceStates.get(room.states, member.user_id)
    {change, states} = VoiceStates.update(room.states, member.user_id, member.session_id, request)

    case change do
      :noop -> {room, []}
      :left -> left(room, states, previous)
      :updated -> {%{room | states: states}, [state_effect(room, states, member.user_id)]}
      _joined_or_moved -> joined(room, states, previous, member, request, join_opts)
    end
  end

  @doc "Removes every state of `session_id` (the session ended)."
  @spec remove_session(t(), String.t()) :: {t(), [effect()]}
  def remove_session(room, session_id) do
    {removed, states} = VoiceStates.remove_session(room.states, session_id)

    effects =
      Enum.map(removed, fn state ->
        {:broadcast_state, VoiceStates.left_payload(state, room.guild_id), state.channel_id}
      end)

    {release_if_empty(%{room | states: states}), effects}
  end

  @doc """
  Reallocates after `node_id` disappeared from etcd and re-sends VOICE_SERVER_UPDATE, each
  participant keeping the E2EE flag of its token. `whereis` maps a session id to its pid.
  """
  @spec node_gone(t(), String.t(), (String.t() -> pid() | nil)) :: {t(), [effect()]}
  def node_gone(%__MODULE__{node_id: node_id} = room, node_id, whereis) do
    Allocator.release(room.server_id, node_id)
    room = %{room | node_id: nil}

    case Allocator.allocate(room.server_id, room.region) do
      {:ok, node} -> {%{room | node_id: node.node_id}, reissue(room, node, whereis)}
      {:error, _reason} -> {room, unavailable(room, whereis)}
    end
  end

  def node_gone(room, _node_id, _whereis), do: {room, []}

  @doc "Stamps a Watch Together command with the server clock and relays it to the channel."
  @spec watch(t(), String.t(), map(), integer()) :: {t(), [effect()]} | :error
  def watch(room, user_id, command, now_ms) do
    if user_in_channel?(room, user_id, command.channel_id) do
      stamped = WatchTogether.stamp(command, user_id, now_ms)

      recipients =
        room.states |> VoiceStates.in_channel(command.channel_id) |> Enum.map(& &1.session_id)

      {%{room | watch: WatchTogether.track(room.watch, stamped)},
       [{:relay_watch, recipients, stamped}]}
    else
      :error
    end
  end

  defp left(room, states, previous) do
    room = release_if_empty(%{room | states: states})

    room =
      if channel_empty?(room, previous.channel_id),
        do: forget_watch(room, previous.channel_id),
        else: room

    {room,
     [{:broadcast_state, VoiceStates.left_payload(previous, room.guild_id), previous.channel_id}]}
  end

  defp joined(room, states, previous, member, request, join_opts) do
    others =
      states
      |> VoiceStates.in_channel(request.channel_id)
      |> Enum.reject(&(&1.user_id == member.user_id))

    case E2eePolicy.setting_flags([member.user_id | Enum.map(others, & &1.user_id)]) do
      {:ok, setting_flags} ->
        admitted(room, states, previous, member, request, join_opts, {others, setting_flags})

      {:error, :settings_unavailable} ->
        refused_for_settings(room, member, request)
    end
  end

  defp admitted(room, states, previous, member, request, join_opts, {others, setting_flags}) do
    case ensure_node(room, join_opts.region) do
      {:ok, room, node} ->
        room = leave_previous_channel(%{room | states: states}, previous, request.channel_id)

        {states, e2ee, upgraded} =
          settle_call_e2ee(room.states, member, others, setting_flags, join_opts)

        room = %{room | states: states}

        effects =
          [
            state_effect(room, states, member.user_id),
            server_update(room, node, VoiceStates.get(states, member.user_id), e2ee, member.pid)
          ] ++
            upgrade_effects(room, node, upgraded, join_opts.whereis) ++
            watch_catch_up(room, request.channel_id, member.pid)

        {room, effects}

      {:error, _reason} ->
        {rejected_room(room, member),
         [{:rejected, member.pid, rejected_payload(room, member, request)}]}
    end
  end

  # A call is E2EE for everyone once the channel requires it, or the joiner or someone already in
  # the channel has the account setting on, or someone already in it is encrypted. Participants
  # who were not encrypted when the call became E2EE are returned in `upgraded`.
  defp settle_call_e2ee(states, member, others, setting_flags, join_opts) do
    encrypted_flags = Enum.map(others, & &1.e2ee)

    e2ee = VoiceTokenClaims.call_e2ee?(join_opts.e2ee, setting_flags ++ encrypted_flags)
    upgraded = if e2ee and not Enum.any?(others, & &1.e2ee), do: others, else: []

    states = VoiceStates.set_e2ee(states, member.user_id, e2ee)
    states = Enum.reduce(upgraded, states, &VoiceStates.set_e2ee(&2, &1.user_id, true))
    {states, e2ee, upgraded}
  end

  defp upgrade_effects(room, node, upgraded, whereis) do
    for state <- upgraded, pid = whereis.(state.session_id), is_pid(pid) do
      server_update(room, node, state, true, pid)
    end
  end

  defp refused_for_settings(room, member, request) do
    join_failed = %{
      "error" => "e2ee_settings_unavailable",
      "guild_id" => room.guild_id,
      "channel_id" => request.channel_id
    }

    {rejected_room(room, member),
     [
       {:rejected, member.pid, rejected_payload(room, member, request)},
       {:join_failed, member.pid, join_failed}
     ]}
  end

  defp rejected_room(room, member) do
    {_removed, states} = VoiceStates.remove_session(room.states, member.session_id)
    release_if_empty(%{room | states: states})
  end

  @doc "The session's own VOICE_STATE_UPDATE telling it that a join was refused."
  @spec rejected_payload(t(), member(), map()) :: map()
  def rejected_payload(room, member, request) do
    VoiceStates.left_payload(
      %{
        user_id: member.user_id,
        session_id: member.session_id,
        channel_id: request.channel_id,
        self_mute: request.self_mute,
        self_deaf: request.self_deaf,
        self_video: request.self_video,
        self_stream: request.self_stream
      },
      room.guild_id
    )
  end

  defp leave_previous_channel(room, %{channel_id: old}, new) when old != new,
    do: if(channel_empty?(room, old), do: forget_watch(room, old), else: room)

  defp leave_previous_channel(room, _previous, _new), do: room

  defp ensure_node(%__MODULE__{node_id: nil} = room, region) do
    with {:ok, node} <- Allocator.allocate(room.server_id, region) do
      {:ok, %{room | node_id: node.node_id, region: region}, node}
    end
  end

  defp ensure_node(room, _region) do
    case Ports.voice_nodes().get_node(room.node_id) do
      {:ok, node} -> {:ok, room, node}
      :error -> ensure_node(%{room | node_id: nil}, room.region)
    end
  end

  defp state_effect(room, states, user_id) do
    state = VoiceStates.get(states, user_id)
    {:broadcast_state, VoiceStates.to_payload(state, room.guild_id), state.channel_id}
  end

  defp server_update(room, %VoiceNode{} = node, state, e2ee, pid) do
    claims =
      VoiceTokenClaims.build(
        %{
          user_id: state.user_id,
          session_id: state.session_id,
          server_id: room.server_id,
          channel_id: state.channel_id,
          node_id: node.node_id,
          e2ee: e2ee,
          guarded_publish: Map.get(state, :guarded_publish, false)
        },
        div(System.os_time(:millisecond), 1000)
      )

    {:server_update, pid,
     %{
       "token" => Ports.voice_token_signer().sign(claims),
       "guild_id" => room.guild_id,
       "channel_id" => state.channel_id,
       "endpoint" => node.ws_url,
       "region" => node.region
     }}
  end

  defp reissue(room, node, whereis) do
    for state <- VoiceStates.all(room.states), pid = whereis.(state.session_id), is_pid(pid) do
      server_update(room, node, state, state.e2ee, pid)
    end
  end

  defp unavailable(room, whereis) do
    for state <- VoiceStates.all(room.states), pid = whereis.(state.session_id), is_pid(pid) do
      {:server_update, pid,
       %{
         "token" => nil,
         "guild_id" => room.guild_id,
         "channel_id" => state.channel_id,
         "endpoint" => nil,
         "region" => room.region
       }}
    end
  end

  defp watch_catch_up(room, channel_id, pid) do
    case Map.get(room.watch, channel_id) do
      nil -> []
      state -> [{:relay_watch_to_pid, pid, state}]
    end
  end

  defp channel_empty?(room, channel_id), do: VoiceStates.in_channel(room.states, channel_id) == []

  defp forget_watch(room, channel_id),
    do: %{room | watch: WatchTogether.forget(room.watch, channel_id)}

  defp release_if_empty(%__MODULE__{node_id: nil} = room), do: room

  defp release_if_empty(room) do
    if VoiceStates.empty?(room.states) do
      Allocator.release(room.server_id, room.node_id)
      %{room | node_id: nil, region: nil, watch: %{}}
    else
      room
    end
  end
end
