defmodule DiscordyGateway.Domain.Voice.VoiceTokenClaims do
  @moduledoc "Claims of the HS256 voice token (P3), valid for 60 seconds."

  alias DiscordyGateway.Contracts

  @type grant :: %{
          user_id: String.t(),
          session_id: String.t(),
          server_id: String.t(),
          channel_id: String.t(),
          node_id: String.t(),
          e2ee: boolean()
        }

  @spec build(grant(), integer()) :: map()
  def build(grant, now_s) do
    %{
      "sub" => grant.user_id,
      "sid" => grant.session_id,
      "srv" => grant.server_id,
      "chn" => grant.channel_id,
      "node" => grant.node_id,
      "e2ee" => grant.e2ee,
      "iat" => now_s,
      "exp" => now_s + Contracts.voice_token_ttl_s()
    }
  end

  @doc "Media E2EE is always on in ephemeral and p2p chats and opt-in (channel `e2ee`) otherwise."
  @spec e2ee_for(map()) :: boolean()
  def e2ee_for(%{"delivery_mode" => mode}) when mode in ["ephemeral", "p2p"], do: true
  def e2ee_for(channel), do: Map.get(channel, "e2ee") == true

  @doc """
  Whether a call is E2EE for everyone. `required` is the channel or delivery-mode requirement;
  `flags` holds one entry per relevant account setting or participant already encrypted. A single
  `true` makes the whole call E2EE (voice#8).
  """
  @spec call_e2ee?(boolean(), [boolean()]) :: boolean()
  def call_e2ee?(required, flags), do: required or Enum.any?(flags)
end
