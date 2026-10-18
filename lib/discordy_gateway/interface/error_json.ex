defmodule DiscordyGateway.Interface.ErrorJSON do
  @moduledoc "JSON error bodies for the HTTP endpoint."

  def render(template, _assigns),
    do: %{"error" => Phoenix.Controller.status_message_from_template(template)}
end
