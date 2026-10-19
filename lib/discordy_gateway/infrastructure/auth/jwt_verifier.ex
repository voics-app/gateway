defmodule DiscordyGateway.Infrastructure.Auth.JwtVerifier do
  @moduledoc """
  Verifies P3 user access tokens: JWT, alg `EdDSA` (Ed25519), issuer `discordy-api`,
  `kind` `user`, not expired. The public key is bound at startup from `JWT_PUBLIC_KEY`.
  """

  @behaviour DiscordyGateway.Application.Ports.TokenVerifier

  @issuer "discordy-api"
  @spki_prefix <<0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00>>

  @doc "Extracts the raw 32-byte Ed25519 public key from a PEM SubjectPublicKeyInfo."
  @spec parse_public_key(String.t()) :: {:ok, binary()} | {:error, :invalid_public_key}
  def parse_public_key(pem) when is_binary(pem) do
    case :public_key.pem_decode(pem) do
      [{:SubjectPublicKeyInfo, <<@spki_prefix, key::binary-size(32)>>, :not_encrypted}] ->
        {:ok, key}

      _ ->
        {:error, :invalid_public_key}
    end
  rescue
    _ -> {:error, :invalid_public_key}
  end

  @impl true
  def verify_access_token(token) do
    key = :discordy_gateway |> Application.fetch_env!(:auth) |> Keyword.fetch!(:jwt_public_key)
    verify(token, key, System.os_time(:second))
  end

  @doc "Verifies `token` against a raw public key at `now_s`."
  @spec verify(String.t(), binary(), integer()) :: {:ok, map()} | {:error, atom()}
  def verify(token, key, now_s) do
    with [header_b64, payload_b64, signature_b64] <- String.split(token, "."),
         {:ok, header} <- decode_json(header_b64),
         :ok <- check_header(header),
         {:ok, signature} <- Base.url_decode64(signature_b64, padding: false),
         :ok <- check_signature(header_b64 <> "." <> payload_b64, signature, key),
         {:ok, claims} <- decode_json(payload_b64) do
      identity(claims, now_s)
    else
      {:error, reason} when is_atom(reason) -> {:error, reason}
      _malformed -> {:error, :malformed}
    end
  end

  defp decode_json(segment) do
    with {:ok, json} <- Base.url_decode64(segment, padding: false),
         {:ok, %{} = map} <- Jason.decode(json) do
      {:ok, map}
    else
      _ -> {:error, :malformed}
    end
  end

  defp check_header(%{"alg" => "EdDSA"}), do: :ok
  defp check_header(_header), do: {:error, :unsupported_algorithm}

  defp check_signature(signing_input, signature, key) do
    if :crypto.verify(:eddsa, :none, signing_input, signature, [key, :ed25519]),
      do: :ok,
      else: {:error, :bad_signature}
  end

  defp identity(%{"iss" => @issuer, "kind" => "user", "sub" => sub, "exp" => exp} = claims, now_s)
       when is_binary(sub) and is_integer(exp) do
    if exp > now_s do
      {:ok, %{user_id: sub, auth_session_id: claims["sid"], device_id: claims["did"]}}
    else
      {:error, :expired}
    end
  end

  defp identity(_claims, _now_s), do: {:error, :invalid_claims}
end
