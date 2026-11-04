defmodule DiscordyGateway.Application.SlowConsumerTest do
  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Session

  setup do
    previous = Application.fetch_env!(:discordy_gateway, :settings)

    Application.put_env(
      :discordy_gateway,
      :settings,
      Keyword.merge(previous, transport_backlog_limit: 10, resume_window_ms: 5_000)
    )

    on_exit(fn -> Application.put_env(:discordy_gateway, :settings, previous) end)
  end

  defp stalled_session do
    transport = spawn(fn -> Process.sleep(:infinity) end)
    {:ok, session} = Session.start(transport)
    identify(session, "1")
    :ok = wait_for(fn -> :sys.get_state(session).status == :identified end)
    {session, transport}
  end

  defp wait_for(fun, attempts \ 50) do
    cond do
      fun.() -> :ok
      attempts == 0 -> flunk("condition not reached")
      true -> Process.sleep(20) && wait_for(fun, attempts - 1)
    end
  end

  test "a socket that never reads is cut off once its backlog passes the limit" do
    {session, transport} = stalled_session()
    handler = "slow-consumer-#{System.unique_integer([:positive])}"
    test = self()

    :telemetry.attach(
      handler,
      [:discordy_gateway, :session, :slow_consumer],
      fn _event, _measurements, _meta, _config -> send(test, :slow_consumer) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    for _ <- 1..200, do: send(session, {:dispatch, "TYPING_START", "{}"})

    assert_receive :slow_consumer, 1_000
    refute Process.alive?(transport)
    assert wait_for(fn -> :sys.get_state(session).transport == nil end) == :ok
  end

  test "the cut session keeps its frames in the resume buffer and stays resumable" do
    {session, _transport} = stalled_session()
    for _ <- 1..200, do: send(session, {:dispatch, "TYPING_START", "{}"})

    :ok = wait_for(fn -> :sys.get_state(session).transport == nil end)
    assert Process.alive?(session)
    assert {:ok, entries} = DiscordyGateway.Fakes.ResumeStore.fetch(:sys.get_state(session).id)
    assert length(entries) >= 100
  end

  test "a reading socket is never cut" do
    client = online("1")
    for _ <- 1..200, do: send(client.session, {:dispatch, "TYPING_START", "{}"})
    assert await(client, "TYPING_START") == %{}
    assert Process.alive?(client.transport)
  end
end
