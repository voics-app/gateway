defmodule DiscordyGateway.Infrastructure.Http.VoiceControl do
  @moduledoc "Node-affine, authenticated revocation of a session's SFU publishing rights."

  alias DiscordyGateway.Domain.Voice.VoiceNode

  @spec revoke(VoiceNode.t(), String.t()) :: :ok | {:error, term()}
  def revoke(%VoiceNode{} = node, session_id) when is_binary(session_id) do
    with %{ip: ip, port: port, lease: lease} <- VoiceNode.validated_control(node) do
      config = Application.fetch_env!(:discordy_gateway, :http)
      options = [
        method: :post,
        url: "http://#{ip}:#{port}/internal/v1/sessions/#{URI.encode(session_id, &URI.char_unreserved?/1)}/revoke",
        headers: [
          {"x-internal-token", Keyword.fetch!(config, :internal_token)},
          {"x-voice-node-id", node.node_id},
          {"x-voice-registration-lease", lease}
        ],
        retry: false,
        redirect: false,
        receive_timeout: 5_000
      ] ++ Keyword.get(config, :req_options, [])
      case Req.request(options) do
        {:ok, %Req.Response{status: 200, body: %{"revoked" => true}}} -> :ok
        {:ok, %Req.Response{status: status}} -> {:error, {:http_status, status}}
        {:error, reason} -> {:error, reason}
      end
    else
      nil -> {:error, :no_trusted_control_endpoint}
    end
  end
end
