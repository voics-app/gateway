defmodule DiscordyGateway.Application.Events.RouterTest do
  use DiscordyGateway.Case, async: false

  alias DiscordyGateway.Application.Events.Router
  alias DiscordyGateway.Domain.Events.EventEnvelope

  test "USER_SETTINGS_UPDATE refreshes the user's e2ee_calls_default flag for voice joins" do
    Fakes.put({:call_default, "2"}, false)

    envelope = %EventEnvelope{
      id: "evt-1",
      type: "USER_SETTINGS_UPDATE",
      target_user_ids: ["2"],
      data: %{"e2ee_calls_default" => true}
    }

    assert :ok = Router.deliver(:user, envelope)
    assert Fakes.get({:call_default, "2"}) == true

    off = %{envelope | id: "evt-2", data: %{"e2ee_calls_default" => false}}
    assert :ok = Router.deliver(:user, off)
    assert Fakes.get({:call_default, "2"}) == false
  end
end
