defmodule DiscordyGateway.Application.Session.Identity do
  @moduledoc "Who a gateway session belongs to, established by IDENTIFY."

  @enforce_keys [:user_id, :kind]
  defstruct [
    :user_id,
    :kind,
    :device_id,
    :application_id,
    :auth_session_id,
    intents: 0,
    platform: "desktop"
  ]

  @type t :: %__MODULE__{
          user_id: String.t(),
          kind: :user | :bot,
          device_id: String.t() | nil,
          application_id: String.t() | nil,
          auth_session_id: String.t() | nil,
          intents: non_neg_integer(),
          platform: String.t()
        }

  @spec bot?(t()) :: boolean()
  def bot?(%__MODULE__{kind: kind}), do: kind == :bot
end
