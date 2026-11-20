defmodule DiscordyGateway.Application.Voice.CapabilitySession do
  @moduledoc """
  Renews a single guarded voice session's node-affine capability lease. The supplied control
  module must acknowledge only after the SFU applies the full update. On failure this process
  tells its owner to disconnect the session; the SFU's expiry bounds a partitioned grant.

  Start only after issuing a token that makes the voice session initially deny publication.
  """
  use GenServer, restart: :temporary

  alias DiscordyGateway.Domain.Voice.Capabilities

  @ttl_ms 15_000
  @renew_ms 5_000
  @retry_ms 200
  @startup_ms 5_000
  @control_timeout_ms 5_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @spec update(pid(), non_neg_integer(), boolean()) :: :ok
  def update(pid, permissions, suppressed),
    do: GenServer.cast(pid, {:update, permissions, suppressed})

  @doc """
  Returns only a current, acknowledged grant for internal trusted callers. This snapshot does not
  replace a fresh authoritative membership and permission check by the caller.
  """
  @spec current(pid()) :: {:ok, map()} | :unavailable
  def current(pid) do
    GenServer.call(pid, :current)
  catch
    :exit, _reason -> :unavailable
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    owner = Keyword.fetch!(opts, :owner)
    node = Keyword.fetch!(opts, :node)
    session_id = Keyword.fetch!(opts, :session_id)
    lease = Keyword.fetch!(opts, :registration_lease)
    Process.monitor(owner)
    token = make_ref()
    send(self(), {:reconcile, token})

    {:ok,
     %{
       owner: owner,
       node: node,
       session_id: session_id,
       control: Keyword.fetch!(opts, :control),
       resolve_node: Keyword.fetch!(opts, :resolve_node),
       policy: Capabilities.new(session_id, node.node_id, lease),
       permissions: Keyword.fetch!(opts, :permissions),
       suppressed: Keyword.fetch!(opts, :suppressed),
       startup_deadline: System.monotonic_time(:millisecond) + @startup_ms,
       timer: nil,
       token: token,
       in_flight: nil
     }}
  end

  @impl true
  def handle_call(:current, _from, state) do
    valid =
      try do
        state.in_flight == nil and state.policy.pending == nil and
          not Capabilities.expired?(state.policy, System.system_time(:millisecond)) and
          current_node?(state)
      rescue
        _error -> false
      catch
        _kind, _reason -> false
      end

    {:reply, if(valid, do: {:ok, state.policy.applied}, else: :unavailable), state}
  end

  @impl true
  def handle_cast({:update, permissions, suppressed}, state) do
    if state.permissions == permissions and state.suppressed == suppressed do
      {:noreply, state}
    else
      # Never send a newer authorization while an earlier request has an uncertain outcome.
      if state.policy.pending do
        fail(state, :changed_during_pending)
      else
        state = cancel_timer(%{state | permissions: permissions, suppressed: suppressed})
        {:noreply, schedule(state, 0)}
      end
    end
  end

  @impl true
  def handle_info({:reconcile, token}, %{token: token} = state) do
    state = %{state | timer: nil, token: nil}

    case command(state) do
      {:ok, state, command} -> start_command(state, command)
      {:error, reason} -> fail(state, reason)
    end
  end

  def handle_info(
        {:control_result, token, result},
        %{in_flight: %{token: token} = flight} = state
      ) do
    Process.cancel_timer(flight.timeout)
    Process.demonitor(flight.ref, [:flush])
    handle_result(%{state | in_flight: nil}, flight.command, flight.update, result)
  end

  def handle_info({:control_timeout, token}, %{in_flight: %{token: token}} = state),
    do: fail(state, :control_timeout)

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{in_flight: %{ref: ref}} = state),
    do: fail(state, :control_failed)

  def handle_info({:DOWN, _ref, :process, owner, _reason}, %{owner: owner} = state),
    do: fail(state, :owner_gone)

  def handle_info(_message, state), do: {:noreply, state}

  defp command(%{policy: %{pending: pending}} = state) when is_map(pending),
    do: {:ok, state, pending}

  defp command(state) do
    case Capabilities.prepare(
           state.policy,
           state.permissions,
           state.suppressed,
           System.system_time(:millisecond),
           @ttl_ms
         ) do
      {:ok, policy, command} -> {:ok, %{state | policy: policy}, command}
      {:error, reason} -> {:error, reason}
    end
  end

  defp start_command(state, command) do
    update = Map.drop(command, ["session_id", "node_id", "registration_lease"])
    parent = self()
    token = make_ref()

    {pid, ref} =
      :erlang.spawn_opt(
        fn -> send(parent, {:control_result, token, control_result(state, update)}) end,
        [:link, :monitor]
      )

    timeout = Process.send_after(self(), {:control_timeout, token}, @control_timeout_ms)

    {:noreply,
     %{
       state
       | in_flight: %{
           pid: pid,
           ref: ref,
           token: token,
           timeout: timeout,
           command: command,
           update: update
         }
     }}
  end

  defp control_result(state, update) do
    try do
      with true <- current_node?(state),
           {:ok, ^update} <- state.control.apply(state.node, state.session_id, update),
           true <- current_node?(state) do
        {:ok, update}
      else
        false -> {:error, :node_changed}
        other -> other
      end
    rescue
      _error -> {:error, :control_exception}
    catch
      _kind, _reason -> {:error, :control_exception}
    end
  end

  defp handle_result(state, command, update, result) do
    case result do
      {:ok, ^update} ->
        case Capabilities.acknowledge(state.policy, command, System.system_time(:millisecond)) do
          {:ok, policy} -> {:noreply, schedule(%{state | policy: policy}, @renew_ms)}
          {:error, reason, _policy, :disconnect} -> fail(state, reason)
        end

      {:error, {:http_status, 503}} ->
        if state.policy.applied == nil and
             System.monotonic_time(:millisecond) < state.startup_deadline do
          {:noreply, schedule(state, @retry_ms)}
        else
          fail(state, :control_unavailable)
        end

      _ ->
        fail(state, :control_failed)
    end
  end

  defp current_node?(state) do
    case state.resolve_node.() do
      {:ok, node} ->
        node.node_id == state.node.node_id and
          Map.get(node, :control_endpoint) == Map.get(state.node, :control_endpoint) and
          get_in(Map.get(node, :control_endpoint) || %{}, [:lease]) ==
            Integer.to_string(state.policy.registration_lease)

      _ ->
        false
    end
  end

  defp schedule(state, delay) do
    token = make_ref()
    %{state | token: token, timer: Process.send_after(self(), {:reconcile, token}, delay)}
  end

  defp cancel_timer(%{timer: nil} = state), do: state

  defp cancel_timer(state) do
    Process.cancel_timer(state.timer)
    %{state | timer: nil, token: nil}
  end

  defp fail(state, reason) do
    if state.in_flight do
      Process.cancel_timer(state.in_flight.timeout)
      Process.demonitor(state.in_flight.ref, [:flush])
      Process.exit(state.in_flight.pid, :kill)
    end

    send(state.owner, {:voice_capability_failed, state.session_id, reason})
    {:stop, :normal, state}
  end

  @impl true
  def terminate(_reason, %{in_flight: %{pid: pid}}) do
    Process.exit(pid, :kill)
    :ok
  end

  def terminate(_reason, _state), do: :ok
end
