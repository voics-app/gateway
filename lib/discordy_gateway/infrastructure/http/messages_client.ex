defmodule DiscordyGateway.Infrastructure.Http.MessagesClient do
  @moduledoc """
  Client of the messages internal API (`/internal/v1`), authenticated with `X-Internal-Token`.
  """

  @behaviour DiscordyGateway.Application.Ports.MessagesClient

  alias DiscordyGateway.Infrastructure.Http.ReadStates

  # Message type CALL.
  @call_type 3

  @impl true
  def fetch_read_states(user_id) do
    with {:ok, response} <-
           request_response(:get, "/internal/v1/users/#{encode(user_id)}/read-states", []) do
      ReadStates.decode(
        response.body,
        Req.Response.get_header(response, "x-read-state-versioned")
      )
    end
  end

  @impl true
  def create_call_message(channel_id, author_id, participants) do
    body = %{
      type: @call_type,
      author_id: author_id,
      call: %{participants: participants, ended_timestamp: nil}
    }

    with {:ok, %{"id" => id}} <-
           request(:post, "/internal/v1/channels/#{encode(channel_id)}/messages", json: body) do
      {:ok, id}
    end
  end

  @impl true
  def update_call_message(channel_id, message_id, update) do
    path = "/internal/v1/channels/#{encode(channel_id)}/messages/#{encode(message_id)}/call"

    with {:ok, _message} <- request(:patch, path, json: update), do: :ok
  end

  defp request(method, path, opts) do
    case request_response(method, path, opts) do
      {:ok, %Req.Response{body: %{} = body}} -> {:ok, body}
      {:ok, _response} -> {:error, :invalid_response}
      {:error, reason} -> {:error, reason}
    end
  end

  defp request_response(method, path, opts) do
    config = Application.fetch_env!(:discordy_gateway, :http)

    options =
      [
        method: method,
        base_url: Keyword.fetch!(config, :messages_url),
        url: path,
        headers: [{"x-internal-token", Keyword.fetch!(config, :internal_token)}],
        retry: false,
        receive_timeout: 5_000
      ] ++ Keyword.get(config, :req_options, []) ++ opts

    case Req.request(options) do
      {:ok, %Req.Response{status: status} = response} when status in 200..201 ->
        {:ok, response}

      {:ok, %Req.Response{status: status}} ->
        {:error, {:http_status, status}}

      {:error, exception} ->
        {:error, exception}
    end
  end

  defp encode(segment), do: URI.encode(segment, &URI.char_unreserved?/1)
end
