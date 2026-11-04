defmodule DiscordyGateway.Application.AdmissionTest do
  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Admission
  alias DiscordyGateway.Fakes.MemoryProbe

  @limit 1_000_000

  setup do
    on_exit(fn ->
      MemoryProbe.reset()
      Admission.sample()
    end)
  end

  defp use_memory(bytes) do
    MemoryProbe.put(bytes, @limit)
    Admission.sample()
  end

  test "admits connections while memory is below the threshold" do
    use_memory(500_000)
    assert Admission.admit?()
  end

  test "refuses connections once memory reaches 75 % of the limit, and counts the refusals" do
    handler = "admission-test-#{System.unique_integer([:positive])}"
    test = self()

    :telemetry.attach(
      handler,
      [:discordy_gateway, :admission, :refused],
      fn _event, measurements, _meta, _config -> send(test, {:refused, measurements.count}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    use_memory(750_000)
    refute Admission.admit?()
    assert_receive {:refused, 1}
  end

  test "reopens only after memory fell 5 points below the threshold" do
    use_memory(800_000)
    refute Admission.admit?()

    use_memory(720_000)
    refute Admission.admit?()

    use_memory(690_000)
    assert Admission.admit?()
  end

  test "reports memory, limit and gate state on every sample" do
    handler = "admission-sample-#{System.unique_integer([:positive])}"
    test = self()

    :telemetry.attach(
      handler,
      [:discordy_gateway, :admission, :sample],
      fn _event, measurements, _meta, _config -> send(test, {:sample, measurements}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    use_memory(900_000)
    assert_receive {:sample, %{memory_bytes: 900_000, memory_limit_bytes: @limit, closed: 1}}
  end

  test "is always open when no limit is known" do
    MemoryProbe.put(999_999_999, nil)
    Admission.sample()
    assert Admission.admit?()
  end
end
