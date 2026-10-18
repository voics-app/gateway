defmodule DiscordyGateway.Case do
  @moduledoc "Shared setup and helpers for application and interface tests."

  use ExUnit.CaseTemplate

  alias DiscordyGateway.Application.Session
  alias DiscordyGateway.Domain.Voice.VoiceNode
  alias DiscordyGateway.Fakes

  using do
    quote do
      import DiscordyGateway.Case

      alias DiscordyGateway.Fakes
    end
  end

  setup do
    Fakes.reset()
    :ok
  end

  @doc "Starts a session whose transport is the calling test process; returns it with the HELLO frame."
  def connect do
    {:ok, session} = Session.start(self())

    receive do
      {:gw_frame, hello} -> {session, decode(hello)}
    after
      1_000 -> raise "no HELLO received"
    end
  end

  def send_frame(session, op, d), do: Session.frame(session, %{"op" => op, "d" => d})

  def identify(session, user_id, extra \\ %{}) do
    d =
      Map.merge(
        %{"token" => "user-" <> user_id, "device_id" => "dev-" <> user_id, "intents" => 0},
        extra
      )

    send_frame(session, 2, d)
  end

  def decode(frame), do: frame |> IO.iodata_to_binary() |> Jason.decode!()

  def next_frame(timeout \\ 500) do
    receive do
      {:gw_frame, frame} -> decode(frame)
    after
      timeout -> :none
    end
  end

  @doc "Skips frames until the named dispatch arrives; returns the frame or `:none`."
  def next_dispatch(event, timeout \\ 500) do
    case next_frame(timeout) do
      :none -> :none
      %{"op" => 0, "t" => ^event} = frame -> frame
      _other -> next_dispatch(event, timeout)
    end
  end

  def flush_frames do
    receive do
      {:gw_frame, _frame} -> flush_frames()
    after
      50 -> :ok
    end
  end

  def voice_node(id, attrs \\ []) do
    struct!(
      VoiceNode,
      [node_id: id, region: "nl-home", ws_url: "ws://#{id}.test", users: 0] ++ attrs
    )
  end

  def unique_id, do: Integer.to_string(System.unique_integer([:positive]) + 1_000_000)
end
