defmodule DiscordyGateway.Application.Session.Authenticate do
  @moduledoc """
  Turns the token of IDENTIFY or RESUME into an identity. User tokens are EdDSA JWTs verified
  locally (P3); `Bot ...` tokens are introspected through the api. User sessions implicitly get
  every intent; bot sessions are limited to the intents they ask for, and may only ask for
  privileged ones the introspection approves (all of them unless it says otherwise).
  """

  import Bitwise

  alias DiscordyGateway.Application.Ports
  alias DiscordyGateway.Application.Session.Identity
  alias DiscordyGateway.Domain.{Intents, Presence}

  @type error :: :authentication_failed | :invalid_intents | :disallowed_intents

  @doc "Authenticates an IDENTIFY payload."
  @spec identify(map()) :: {:ok, Identity.t()} | {:error, error()}
  def identify(%{token: token, device_id: device_id, intents: intents, properties: properties}) do
    platform = Presence.platform_for(properties)

    with {:ok, identity} <- authenticate_token(token) do
      finish(identity, device_id, intents, platform)
    end
  end

  @doc "Authenticates the token of a RESUME payload."
  @spec authenticate_token(String.t()) :: {:ok, map()} | {:error, :authentication_failed}
  def authenticate_token("Bot " <> token), do: introspect_bot(token)
  def authenticate_token("Bearer " <> token), do: verify_user(token)
  def authenticate_token(token), do: verify_user(token)

  defp verify_user(token) do
    case Ports.token_verifier().verify_access_token(token) do
      {:ok, identity} ->
        {:ok,
         Map.merge(identity, %{kind: :user, application_id: nil, approved_intents: Intents.all()})}

      {:error, _reason} ->
        {:error, :authentication_failed}
    end
  end

  defp introspect_bot(token) do
    case Ports.api_client().introspect(token) do
      {:ok, %{"kind" => "bot", "user_id" => user_id} = response} ->
        {:ok,
         %{
           user_id: user_id,
           kind: :bot,
           device_id: nil,
           auth_session_id: response["session_id"],
           application_id: response["application_id"],
           approved_intents: approved_intents(response)
         }}

      _other ->
        {:error, :authentication_failed}
    end
  end

  defp approved_intents(%{"approved_intents" => value}) when is_integer(value), do: value
  defp approved_intents(_response), do: Intents.all()

  defp finish(%{kind: :user} = identity, device_id, _intents, platform) do
    {:ok,
     %Identity{
       user_id: identity.user_id,
       kind: :user,
       device_id: device_id || identity.device_id,
       auth_session_id: identity.auth_session_id,
       intents: Intents.all(),
       platform: platform
     }}
  end

  defp finish(%{kind: :bot} = identity, _device_id, intents, platform) do
    with :ok <- Intents.validate(intents),
         :ok <- Intents.check_privileged(intents, identity.approved_intents &&& Intents.all()) do
      {:ok,
       %Identity{
         user_id: identity.user_id,
         kind: :bot,
         application_id: identity.application_id,
         auth_session_id: identity.auth_session_id,
         intents: intents,
         platform: platform
       }}
    end
  end
end
