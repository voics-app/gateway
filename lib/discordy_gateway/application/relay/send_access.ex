defmodule DiscordyGateway.Application.Relay.SendAccess do
  @moduledoc false

  alias DiscordyGateway.Domain.Permissions

  # API supplies the effective permissions, including screening restrictions.
  def authorize(access) do
    permissions = Permissions.parse(access["permissions"])

    send_permission =
      if access["channel_type"] in [10, 11, 12],
        do: :send_messages_in_threads,
        else: :send_messages

    if Permissions.has?(permissions, :view_channel) and
         Permissions.has?(permissions, send_permission),
       do: :ok,
       else: {:error, :missing_permissions}
  end
end
