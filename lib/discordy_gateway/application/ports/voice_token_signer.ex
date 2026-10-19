defmodule DiscordyGateway.Application.Ports.VoiceTokenSigner do
  @moduledoc "Signs voice tokens (P3, HS256 with VOICE_TOKEN_SECRET)."

  @callback sign(claims :: map()) :: String.t()
end
