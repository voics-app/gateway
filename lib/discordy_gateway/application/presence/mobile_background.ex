defmodule DiscordyGateway.Application.Presence.MobileBackground do
  @moduledoc "Bounded mobile availability after a client explicitly reports OS backgrounding."

  alias DiscordyGateway.Application.{Ports, Settings}
  alias DiscordyGateway.Application.Session.Identity

  def update(
        %{identity: %Identity{platform: "mobile", kind: :user}} = state,
        %{
          background: true
        } = presence
      ) do
    clear_timer(state)
    token = make_ref()
    duration = Settings.mobile_background_presence_ms()
    timer = Process.send_after(self(), {:background_presence_expired, token}, duration)

    %{
      state
      | background_token: token,
        background_timer: timer,
        background_online: presence.status == "online"
    }
  end

  def update(state, _presence), do: clear(state)

  def clear(state) do
    clear_timer(state)
    %{state | background_token: nil, background_timer: nil, background_online: false}
  end

  def retained?(state), do: state.background_token != nil

  def account_status(state, %{"status" => status}) when status in ~w(online idle dnd invisible) do
    automatic_idle = status == "online" and not state.background_online
    effective = if automatic_idle, do: "idle", else: status
    presence = %{state.presence | status: effective, afk: automatic_idle}
    %{state | manual_status: status, presence: presence}
  end

  def replace_previous(%Identity{platform: "mobile", kind: :user} = identity) do
    for pid <- Ports.session_directory().sessions_for_user(identity.user_id), pid != self() do
      replace(pid, identity)
    end
  end

  def replace_previous(_identity), do: :ok

  def same_login?(state, identity) do
    retained?(state) and state.identity.user_id == identity.user_id and
      state.identity.auth_session_id == identity.auth_session_id and
      state.identity.device_id == identity.device_id
  end

  defp replace(pid, identity) do
    GenServer.call(pid, {:replace_background_session, identity})
  catch
    :exit, _reason -> :ok
  end

  defp clear_timer(%{background_timer: nil}), do: :ok
  defp clear_timer(state), do: Process.cancel_timer(state.background_timer)
end
