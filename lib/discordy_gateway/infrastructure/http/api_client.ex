defmodule DiscordyGateway.Infrastructure.Http.ApiClient do
  @moduledoc """
  Client of the api's internal HTTP API (P4), authenticated with `X-Internal-Token`. Bot token
  introspection results are cached for 60 seconds (P3).
  """

  @behaviour DiscordyGateway.Application.Ports.ApiClient

  @cache :discordy_introspect_cache
  @cache_ttl_ms 60_000

  @doc "Creates the introspection cache table (called by the composition root)."
  @spec init_cache() :: :ok
  def init_cache do
    :ets.new(@cache, [:named_table, :public, :set, read_concurrency: true])
    :ok
  end

  @impl true
  def introspect(token) do
    key = :crypto.hash(:sha256, token)
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(@cache, key) do
      [{^key, expires, response}] when expires > now ->
        {:ok, response}

      _stale_or_missing ->
        with {:ok, response} <-
               request(:post, "/internal/v1/auth/introspect", json: %{token: token}) do
          :ets.insert(@cache, {key, now + @cache_ttl_ms, response})
          {:ok, response}
        end
    end
  end

  @impl true
  def fetch_ready(user_id), do: request(:get, "/internal/v1/users/#{encode(user_id)}/ready")

  @impl true
  def fetch_guild_state(guild_id),
    do: request(:get, "/internal/v1/guilds/#{encode(guild_id)}/state")

  @impl true
  def fetch_channel_access(channel_id, user_id),
    do: request(:get, "/internal/v1/channels/#{encode(channel_id)}/access/#{encode(user_id)}")

  defp request(method, path, opts \\ []) do
    config = Application.fetch_env!(:discordy_gateway, :http)

    options =
      [
        method: method,
        base_url: Keyword.fetch!(config, :api_url),
        url: path,
        headers: [{"x-internal-token", Keyword.fetch!(config, :internal_token)}],
        retry: false,
        receive_timeout: 5_000
      ] ++ Keyword.get(config, :req_options, []) ++ opts

    case Req.request(options) do
      {:ok, %Req.Response{status: 200, body: %{} = body}} -> {:ok, body}
      {:ok, %Req.Response{status: 401}} -> {:error, :unauthorized}
      {:ok, %Req.Response{status: 403}} -> {:error, :forbidden}
      {:ok, %Req.Response{status: 404}} -> {:error, :not_found}
      {:ok, %Req.Response{status: status}} -> {:error, {:http_status, status}}
      {:error, exception} -> {:error, exception}
    end
  end

  defp encode(segment), do: URI.encode(segment, &URI.char_unreserved?/1)
end
