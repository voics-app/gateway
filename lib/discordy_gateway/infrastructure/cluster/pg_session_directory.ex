defmodule DiscordyGateway.Infrastructure.Cluster.PgSessionDirectory do
  @moduledoc """
  Cluster-wide session lookup on OTP `:pg`. A session joins the groups `{:session, id}`,
  `{:user, id}`, `{:device, id}` and `:sessions` from its own process, so entries disappear
  with it.
  """

  @behaviour DiscordyGateway.Application.Ports.SessionDirectory

  @scope DiscordyGateway.Pg

  @impl true
  def register(session_id, user_id, device_id) do
    groups = [{:session, session_id}, {:user, user_id}, :sessions]
    groups = if device_id, do: [{:device, device_id} | groups], else: groups
    Enum.each(groups, &:pg.join(@scope, &1, self()))
    :ok
  end

  @impl true
  def whereis(session_id) do
    case :pg.get_members(@scope, {:session, session_id}) do
      [pid | _] -> pid
      [] -> nil
    end
  end

  @impl true
  def sessions_for_user(user_id), do: :pg.get_members(@scope, {:user, user_id})

  @impl true
  def sessions_for_device(device_id), do: :pg.get_members(@scope, {:device, device_id})

  @impl true
  def count_by_node do
    @scope
    |> :pg.get_members(:sessions)
    |> Enum.frequencies_by(&Atom.to_string(node(&1)))
  end
end
