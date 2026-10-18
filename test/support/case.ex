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

  @doc "A named client: a transport process that tags what its session sends and reports it to the test."
  def client(name) do
    test = self()

    transport =
      spawn_link(fn ->
        forward = fn forward ->
          receive do
            {:gw_frame, frame} -> send(test, {:client, name, decode(frame)})
            {:gw_close, code} -> send(test, {:closed, name, code})
          end

          forward.(forward)
        end

        forward.(forward)
      end)

    {:ok, session} = Session.start(transport)
    %{name: name, transport: transport, session: session}
  end

  @doc "Connects a client for `user_id`, identifies, and waits for READY."
  def online(user_id, extra \\ %{}) do
    client = client(user_id)
    identify(client.session, user_id, extra)
    await(client, "READY")
    client
  end

  @doc "Waits for a dispatch to `client`; returns its payload `d`, or `:none` on timeout."
  def await(client, event, timeout \\ 500) do
    name = client.name

    receive do
      {:client, ^name, %{"op" => 0, "t" => ^event, "d" => d}} -> d
    after
      timeout -> :none
    end
  end

  @doc "Waits for the PRESENCE_UPDATE about `user_id` with `status`."
  def await_presence(client, user_id, status, timeout \\ 500) do
    case await(client, "PRESENCE_UPDATE", timeout) do
      %{"user" => %{"id" => ^user_id}, "status" => ^status} = payload -> payload
      :none -> :none
      _other -> await_presence(client, user_id, status, timeout)
    end
  end

  @doc "Waits for a non-dispatch frame with the given opcode."
  def await_op(client, op, timeout \\ 500) do
    name = client.name

    receive do
      {:client, ^name, %{"op" => ^op} = frame} -> frame
    after
      timeout -> :none
    end
  end

  def await_close(client, timeout \\ 500) do
    name = client.name

    receive do
      {:closed, ^name, code} -> code
    after
      timeout -> :none
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
