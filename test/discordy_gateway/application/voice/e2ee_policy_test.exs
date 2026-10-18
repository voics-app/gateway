defmodule DiscordyGateway.Application.Voice.E2eePolicyTest do
  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Voice.E2eePolicy
  alias DiscordyGateway.Fakes

  setup do
    previous = Application.get_env(:discordy_gateway, :settings)
    on_exit(fn -> Application.put_env(:discordy_gateway, :settings, previous) end)
    :ok
  end

  defp configure(policy, attempts \\ 3) do
    settings =
      :discordy_gateway
      |> Application.get_env(:settings, [])
      |> Keyword.merge(
        e2ee_settings_unavailable_policy: policy,
        e2ee_settings_wait_attempts: attempts,
        e2ee_settings_wait_interval_ms: 1
      )

    Application.put_env(:discordy_gateway, :settings, settings)
  end

  defp fail_reads(count), do: Fakes.put(:call_defaults_failures, count)

  defp read_attempts, do: length(Fakes.calls(:call_defaults))

  test "a healthy read returns one flag per user, in order, whatever the policy" do
    configure(:open)
    Fakes.put({:call_default, "2"}, true)

    assert {:ok, [false, true]} = E2eePolicy.setting_flags(["1", "2"])
    assert read_attempts() == 1
  end

  describe "open (default)" do
    test "an unavailable read counts every user as not opted in, with a single attempt" do
      configure(:open)
      fail_reads(99)

      assert {:ok, [false, false]} = E2eePolicy.setting_flags(["1", "2"])
      assert read_attempts() == 1
    end
  end

  describe "closed" do
    test "an unavailable read counts every user as opted in, so the call is encrypted" do
      configure(:closed)
      fail_reads(99)

      assert {:ok, [true, true]} = E2eePolicy.setting_flags(["1", "2"])
      assert read_attempts() == 1
    end
  end

  describe "wait" do
    test "retries the read and succeeds when it recovers within the attempts" do
      configure(:wait)
      Fakes.put({:call_default, "2"}, true)
      fail_reads(2)

      assert {:ok, [false, true]} = E2eePolicy.setting_flags(["1", "2"])
      assert read_attempts() == 3
    end

    test "gives up after the configured attempts and reports the settings unavailable" do
      configure(:wait, 3)
      fail_reads(99)

      assert {:error, :settings_unavailable} = E2eePolicy.setting_flags(["1", "2"])
      assert read_attempts() == 3
    end
  end
end
