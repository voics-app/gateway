defmodule DiscordyGateway.Application.Voice.CapabilitySessionTest do
  use ExUnit.Case, async: true

  import Bitwise
  alias DiscordyGateway.Application.Voice.CapabilitySession
  alias DiscordyGateway.Contracts

  defmodule Control do
    def apply(node, session_id, update) do
      send(node.test_pid, {:control, self(), session_id, update})

      receive do
        {:reply, reply} -> reply
      after
        1_000 -> {:error, :timeout}
      end
    end
  end

  defp permissions(names), do: Enum.reduce(names, 0, &bor(Contracts.permission(&1), &2))

  defp await_current(pid, attempts \\ 50)
  defp await_current(_pid, 0), do: :unavailable

  defp await_current(pid, attempts) do
    case CapabilitySession.current(pid) do
      :unavailable ->
        Process.sleep(10)
        await_current(pid, attempts - 1)

      result ->
        result
    end
  end

  test "retries initial unknown session, applies grant, then revokes with a newer revision" do
    perms = permissions([:view_channel, :connect, :speak, :stream])
    node = %{node_id: "node", test_pid: self(), control_endpoint: %{lease: "12"}}

    {:ok, pid} =
      CapabilitySession.start_link(
        owner: self(),
        node: node,
        resolve_node: fn -> {:ok, node} end,
        session_id: "session",
        registration_lease: 12,
        control: Control,
        permissions: perms,
        suppressed: false
      )

    assert_receive {:control, worker, "session", first}
    assert first["revision"] == 1
    assert first["can_publish_video"]
    send(worker, {:reply, {:error, {:http_status, 503}}})
    assert_receive {:control, worker, "session", ^first}, 1_000
    assert CapabilitySession.current(pid) == :unavailable
    send(worker, {:reply, {:ok, first}})
    assert {:ok, %{"revision" => 1}} = await_current(pid)

    CapabilitySession.update(pid, permissions([:view_channel, :connect, :speak]), false)
    assert_receive {:control, worker, "session", second}
    assert second["revision"] == 2
    refute second["can_publish_video"]
    send(worker, {:reply, {:ok, second}})
    refute_receive {:voice_capability_failed, _, _}, 50
    GenServer.stop(pid)
  end

  test "failed or mismatched acknowledgment tells the owner to disconnect" do
    node = %{node_id: "node", test_pid: self(), control_endpoint: %{lease: "12"}}

    {:ok, _pid} =
      CapabilitySession.start_link(
        owner: self(),
        node: node,
        resolve_node: fn -> {:ok, node} end,
        session_id: "session",
        registration_lease: 12,
        control: Control,
        permissions: 0,
        suppressed: false
      )

    assert_receive {:control, worker, "session", update}
    send(worker, {:reply, {:ok, Map.put(update, "revision", 999)}})
    assert_receive {:voice_capability_failed, "session", :control_failed}
  end

  test "a node lease change during an acknowledgment fails closed" do
    node = %{node_id: "node", test_pid: self(), control_endpoint: %{lease: "12"}}
    {:ok, registry} = Agent.start_link(fn -> node end)

    {:ok, _pid} =
      CapabilitySession.start_link(
        owner: self(),
        node: node,
        resolve_node: fn -> {:ok, Agent.get(registry, & &1)} end,
        session_id: "session",
        registration_lease: 12,
        control: Control,
        permissions: 0,
        suppressed: false
      )

    assert_receive {:control, worker, "session", update}
    Agent.update(registry, &put_in(&1.control_endpoint.lease, "13"))
    send(worker, {:reply, {:ok, update}})
    assert_receive {:voice_capability_failed, "session", :control_failed}
  end

  test "a permission change during an unacknowledged request disconnects immediately" do
    node = %{node_id: "node", test_pid: self(), control_endpoint: %{lease: "12"}}

    {:ok, pid} =
      CapabilitySession.start_link(
        owner: self(),
        node: node,
        resolve_node: fn -> {:ok, node} end,
        session_id: "session",
        registration_lease: 12,
        control: Control,
        permissions: 0,
        suppressed: false
      )

    assert_receive {:control, worker, "session", _update}
    monitor = Process.monitor(worker)
    CapabilitySession.update(pid, permissions([:view_channel, :connect, :stream]), false)
    assert_receive {:voice_capability_failed, "session", :changed_during_pending}
    assert_receive {:DOWN, ^monitor, :process, ^worker, :killed}
  end
end

