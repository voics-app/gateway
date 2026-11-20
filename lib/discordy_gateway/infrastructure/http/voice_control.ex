defmodule DiscordyGateway.Infrastructure.Http.VoiceControl do
  @moduledoc "Node-affine, authenticated revocation of a session's SFU publishing rights."

  alias DiscordyGateway.Domain.Voice.VoiceNode

  @doc "Applies the complete bounded grant and returns it only after the node ACKs."
  @spec apply(VoiceNode.t(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def apply(%VoiceNode{} = node, session_id, update) when is_binary(session_id) and is_map(update) do
    with %{ip: ip, port: port, lease: lease} <- VoiceNode.validated_control(node) do
      config = Application.fetch_env!(:discordy_gateway, :http)
      options = [
        method: :post,
        url: "http://#{ip}:#{port}/internal/v1/sessions/#{URI.encode(session_id, &URI.char_unreserved?/1)}/publish-permissions",
        headers: [
          {"x-internal-token", Keyword.fetch!(config, :internal_token)},
          {"x-voice-node-id", node.node_id},
          {"x-voice-registration-lease", lease}
        ],
        json: update,
        retry: false,
        redirect: false,
        receive_timeout: 5_000
      ] ++ Keyword.get(config, :req_options, [])

      case Req.request(options) do
        {:ok, %Req.Response{status: 200, body: %{"applied" => true, "session_id" => ^session_id, "revision" => revision}}} ->
          if revision == Map.get(update, :revision) or revision == Map.get(update, "revision"),
            do: {:ok, update},
            else: {:error, :invalid_ack}
        {:ok, %Req.Response{status: status}} -> {:error, {:http_status, status}}
        {:error, reason} -> {:error, reason}
      end
    else
      nil -> {:error, :no_trusted_control_endpoint}
    end
  end

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
