defmodule DiscordyGateway.Infrastructure.Runtime.MemoryProbeTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Infrastructure.Runtime.MemoryProbe

  test "reads a cgroup limit" do
    assert MemoryProbe.parse_limit("1073741824\n") == 1_073_741_824
  end

  test "treats cgroup v2 `max` and the cgroup v1 unlimited value as no limit" do
    assert MemoryProbe.parse_limit("max\n") == nil
    assert MemoryProbe.parse_limit("9223372036854771712\n") == nil
    assert MemoryProbe.parse_limit("0") == nil
  end

  test "usage is the VM's allocated memory" do
    assert MemoryProbe.used_bytes() > 0
  end
end
