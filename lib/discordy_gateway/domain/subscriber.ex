defmodule DiscordyGateway.Domain.Subscriber do
  @moduledoc "A session subscribed to a guild, as seen by fan-out."

  @enforce_keys [:pid, :user_id, :intents]
  defstruct [:pid, :user_id, :intents, :session_id, bot: false, application_id: nil]

  @type t :: %__MODULE__{
          pid: term(),
          user_id: String.t(),
          intents: non_neg_integer(),
          session_id: String.t() | nil,
          bot: boolean(),
          application_id: String.t() | nil
        }
end
