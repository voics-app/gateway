defmodule DiscordyGateway.Application.Events.Router do
  @moduledoc """
  P6 routing of a consumed envelope. A guild id without targets goes to the guild process (which
  filters by VIEW_CHANNEL and intents); otherwise the event goes to every session of each
  targeted user. Interaction events go to the sessions of the target application's bot user.
  """

  alias DiscordyGateway.Application.{Guild, Ports}
  alias DiscordyGateway.Domain.Events.EventEnvelope

  @spec deliver(atom(), EventEnvelope.t()) :: :ok
  def deliver(:interaction, %EventEnvelope{} = envelope), do: deliver_interaction(envelope)

  def deliver(_topic, %EventEnvelope{} = envelope) do
    case EventEnvelope.route(envelope) do
      {:users, user_ids} -> deliver_to_users(user_ids, envelope)
      {:guild, guild_id} -> deliver_to_guild(guild_id, envelope)
      :drop -> :ok
    end
  end

  defp deliver_to_users(user_ids, envelope) do
    for user_id <- Enum.uniq(user_ids),
        pid <- Ports.session_directory().sessions_for_user(user_id) do
      send(pid, {:user_event, envelope})
    end

    :ok
  end

  defp deliver_to_guild(guild_id, %EventEnvelope{type: "GUILD_MEMBER_ADD"} = envelope) do
    _ = Guild.event(guild_id, envelope)

    case get_in(envelope.data, ["user", "id"]) do
      nil -> :ok
      user_id -> notify_join(user_id, guild_id)
    end
  end

  defp deliver_to_guild(guild_id, envelope) do
    _ = Guild.event(guild_id, envelope)
    :ok
  end

  defp notify_join(user_id, guild_id) do
    for pid <- Ports.session_directory().sessions_for_user(user_id),
        do: send(pid, {:join_guild, guild_id})

    :ok
  end

  defp deliver_interaction(envelope) do
    case envelope.target_user_ids || List.wrap(envelope.data["application_id"]) do
      [] -> :ok
      user_ids -> deliver_to_users(user_ids, envelope)
    end
  end
end
