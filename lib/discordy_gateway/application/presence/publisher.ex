defmodule DiscordyGateway.Application.Presence.Publisher do
  @moduledoc """
  Presence across all of a user's sessions. Each session's presence is stored; the aggregate
  (`Domain.Presence`) is compared with the last broadcast one, and only a change is fanned out to
  the user's guilds and to the sessions of the user's friends.
  """

  alias DiscordyGateway.Application.{Dispatch, Guild, Ports}
  alias DiscordyGateway.Domain.Presence

  @type context :: %{
          user_id: String.t(),
          session_id: String.t(),
          guild_ids: [String.t()],
          friend_ids: [String.t()]
        }

  @doc "Records the presence of one session and broadcasts the aggregate if it changed."
  @spec update(context(), Presence.session_presence()) :: :ok
  def update(ctx, presence) do
    case Ports.presence_store().put(ctx.user_id, ctx.session_id, presence) do
      {:ok, sessions} -> broadcast_if_changed(ctx, sessions)
      {:error, _reason} -> :ok
    end
  end

  @doc "Forgets the presence of a session that ended."
  @spec drop(context()) :: :ok
  def drop(ctx) do
    case Ports.presence_store().remove(ctx.user_id, ctx.session_id) do
      {:ok, sessions} -> broadcast_if_changed(ctx, sessions)
      {:error, _reason} -> :ok
    end
  end

  @doc "Refreshes only this session and broadcasts removal of other expired session entries."
  def refresh(ctx) do
    case Ports.presence_store().refresh(ctx.user_id, ctx.session_id) do
      {:ok, sessions} -> broadcast_if_changed(ctx, sessions)
      {:error, _reason} -> :ok
    end
  end

  @doc "Presence of a user's friends, as PRESENCE_UPDATE payloads for READY."
  @spec friend_payloads([String.t()]) :: [map()]
  def friend_payloads(friend_ids) do
    friend_ids
    |> Ports.presence_store().get_aggregates()
    |> Enum.map(fn {user_id, aggregate} -> Presence.dispatch_payload(user_id, aggregate, nil) end)
  end

  defp broadcast_if_changed(ctx, sessions) do
    aggregate = Presence.aggregate(sessions)

    case Ports.presence_store().swap_aggregate(ctx.user_id, aggregate) do
      :changed -> broadcast(ctx, aggregate)
      :unchanged -> :ok
    end
  end

  defp broadcast(ctx, aggregate) do
    payload = Presence.dispatch_payload(ctx.user_id, aggregate, nil)
    Enum.each(ctx.guild_ids, &Guild.presence(&1, payload, aggregate.afk))

    friend_pids =
      Enum.flat_map(ctx.friend_ids, fn id -> Ports.session_directory().sessions_for_user(id) end)

    Dispatch.to_sessions(friend_pids, "PRESENCE_UPDATE", payload)
  end
end
