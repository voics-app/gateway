defmodule DiscordyGateway.Infrastructure.AuthTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Infrastructure.Auth.{
    CoturnCredentials,
    Hs256VoiceTokenSigner,
    JwtVerifier
  }

  @spki_prefix <<0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00>>

  defp keypair, do: :crypto.generate_key(:eddsa, :ed25519)

  defp jwt(private, claims, header \\ %{"alg" => "EdDSA", "typ" => "JWT"}) do
    encode = &(&1 |> Jason.encode!() |> Base.url_encode64(padding: false))
    input = encode.(header) <> "." <> encode.(claims)
    signature = :crypto.sign(:eddsa, :none, input, [private, :ed25519])
    input <> "." <> Base.url_encode64(signature, padding: false)
  end

  defp claims(overrides \\ %{}) do
    Map.merge(
      %{
        "iss" => "discordy-api",
        "sub" => "42",
        "sid" => "s1",
        "did" => "d1",
        "kind" => "user",
        "exp" => 2_000_000_000
      },
      overrides
    )
  end

  describe "JwtVerifier" do
    test "accepts a valid EdDSA user token" do
      {public, private} = keypair()

      assert {:ok, %{user_id: "42", auth_session_id: "s1", device_id: "d1"}} =
               JwtVerifier.verify(jwt(private, claims()), public, 1_900_000_000)
    end

    test "rejects expired tokens, wrong issuers, non-user kinds and other algorithms" do
      {public, private} = keypair()

      assert {:error, :expired} =
               JwtVerifier.verify(jwt(private, claims()), public, 2_000_000_001)

      assert {:error, :invalid_claims} =
               JwtVerifier.verify(jwt(private, claims(%{"iss" => "evil"})), public, 1)

      assert {:error, :invalid_claims} =
               JwtVerifier.verify(jwt(private, claims(%{"kind" => "bot"})), public, 1)

      assert {:error, :unsupported_algorithm} =
               JwtVerifier.verify(jwt(private, claims(), %{"alg" => "HS256"}), public, 1)
    end

    test "rejects tokens signed by another key and malformed tokens" do
      {public, _private} = keypair()
      {_other_public, other_private} = keypair()

      assert {:error, :bad_signature} =
               JwtVerifier.verify(jwt(other_private, claims()), public, 1)

      assert {:error, :malformed} = JwtVerifier.verify("not-a-jwt", public, 1)
      assert {:error, :malformed} = JwtVerifier.verify("a.b.c", public, 1)
    end

    test "parses an Ed25519 public key PEM" do
      {public, _private} = keypair()

      pem =
        :public_key.pem_encode([{:SubjectPublicKeyInfo, @spki_prefix <> public, :not_encrypted}])

      assert {:ok, ^public} = JwtVerifier.parse_public_key(pem)
      assert {:error, :invalid_public_key} = JwtVerifier.parse_public_key("garbage")
    end
  end

  describe "Hs256VoiceTokenSigner" do
    test "produces a verifiable HS256 JWT carrying the claims" do
      token = Hs256VoiceTokenSigner.sign(%{"sub" => "42", "node" => "n1"}, "secret-secret-secret")
      [header, payload, signature] = String.split(token, ".")

      assert %{"alg" => "HS256"} = header |> Base.url_decode64!(padding: false) |> Jason.decode!()

      assert %{"sub" => "42", "node" => "n1"} =
               payload |> Base.url_decode64!(padding: false) |> Jason.decode!()

      expected = :crypto.mac(:hmac, :sha256, "secret-secret-secret", header <> "." <> payload)
      assert Base.url_decode64!(signature, padding: false) == expected
    end
  end

  describe "CoturnCredentials" do
    test "follows the coturn REST scheme: username is expiry:user, credential is HMAC-SHA1 base64" do
      creds =
        CoturnCredentials.issue("42", 1_000_000, "turn-secret", [
          "turn:t.example:3478?transport=udp"
        ])

      assert creds.username == "1003600:42"
      assert creds.ttl == 3600
      assert creds.urls == ["turn:t.example:3478?transport=udp"]

      assert creds.credential ==
               :hmac |> :crypto.mac(:sha, "turn-secret", "1003600:42") |> Base.encode64()
    end
  end
end
