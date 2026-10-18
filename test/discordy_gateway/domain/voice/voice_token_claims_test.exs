defmodule DiscordyGateway.Domain.Voice.VoiceTokenClaimsTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Voice.VoiceTokenClaims

  test "builds the P3 voice token claims with a 60 s lifetime" do
    claims =
      VoiceTokenClaims.build(
        %{
          user_id: "u1",
          session_id: "s1",
          server_id: "g1",
          channel_id: "c1",
          node_id: "n1",
          e2ee: true
        },
        1_000
      )

    assert claims == %{
             "sub" => "u1",
             "sid" => "s1",
             "srv" => "g1",
             "chn" => "c1",
             "node" => "n1",
             "e2ee" => true,
             "iat" => 1_000,
             "exp" => 1_060
           }
  end

  test "e2ee_for/1 is on for ephemeral and p2p delivery, opt-in for standard" do
    assert VoiceTokenClaims.e2ee_for(%{"delivery_mode" => "ephemeral"})
    assert VoiceTokenClaims.e2ee_for(%{"delivery_mode" => "p2p"})
    refute VoiceTokenClaims.e2ee_for(%{"delivery_mode" => "standard"})
    assert VoiceTokenClaims.e2ee_for(%{"delivery_mode" => "standard", "e2ee" => true})
    refute VoiceTokenClaims.e2ee_for(%{})
  end
end
