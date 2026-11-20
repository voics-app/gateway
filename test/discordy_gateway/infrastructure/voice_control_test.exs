defmodule DiscordyGateway.Infrastructure.VoiceControlTest do
  use ExUnit.Case, async: false
  import Plug.Conn
  alias DiscordyGateway.Domain.Voice.VoiceNode
  alias DiscordyGateway.Infrastructure.Http.VoiceControl

  setup do
    previous = Application.fetch_env!(:discordy_gateway, :http)

    Application.put_env(
      :discordy_gateway,
      :http,
      Keyword.merge(previous,
        internal_token: "secret",
        req_options: [plug: {Req.Test, __MODULE__}]
      )
    )

    on_exit(fn -> Application.put_env(:discordy_gateway, :http, previous) end)
    :ok
  end

  test "uses private node address and the current lease, and requires an acknowledgment" do
    node = %VoiceNode{
      node_id: "n",
      region: "r",
      ws_url: "wss://public.test",
      control_endpoint: %{ip: "10.42.0.8", port: 4100, lease: "123"}
    }

    Req.Test.stub(__MODULE__, fn conn ->
      assert conn.host == "10.42.0.8"
      assert conn.request_path == "/internal/v1/sessions/session-1/revoke"
      assert get_req_header(conn, "x-internal-token") == ["secret"]
      assert get_req_header(conn, "x-voice-node-id") == ["n"]
      assert get_req_header(conn, "x-voice-registration-lease") == ["123"]
      Req.Test.json(conn, %{"revoked" => true})
    end)

    assert VoiceControl.revoke(node, "session-1") == :ok

    assert {:error, :no_trusted_control_endpoint} =
             VoiceControl.revoke(
               %{node | control_endpoint: %{ip: "203.0.113.9", port: 4100, lease: "123"}},
               "session-1"
             )

    Req.Test.stub(__MODULE__, &send_resp(&1, 403, "{}"))
    assert {:error, {:http_status, 403}} = VoiceControl.revoke(node, "session-1")
  end

  test "atomic permissions require matching node acknowledgement" do
    node = %VoiceNode{
      node_id: "n",
      region: "r",
      ws_url: "wss://public.test",
      control_endpoint: %{ip: "10.42.0.8", port: 4100, lease: "123"}
    }

    update = %{
      revision: 3,
      expires_at: 1234,
      can_publish_audio: true,
      can_publish_video: false,
      can_publish_soundboard: false
    }

    Req.Test.stub(__MODULE__, fn conn ->
      assert conn.request_path == "/internal/v1/sessions/session-1/publish-permissions"
      assert get_req_header(conn, "x-voice-registration-lease") == ["123"]
      Req.Test.json(conn, %{"applied" => true, "session_id" => "session-1", "revision" => 3})
    end)

    assert {:ok, ^update} = VoiceControl.apply(node, "session-1", update)

    Req.Test.stub(__MODULE__, fn conn ->
      Req.Test.json(conn, %{"applied" => true, "session_id" => "session-1", "revision" => 2})
    end)

    assert {:error, :invalid_ack} = VoiceControl.apply(node, "session-1", update)
  end
end
