defmodule DiscordyGateway.Interface.InternalAuth do
  @moduledoc "Requires the shared `X-Internal-Token` on internal endpoints (compared in constant time)."

  @behaviour Plug

  import Plug.Conn

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    expected =
      :discordy_gateway |> Application.fetch_env!(:http) |> Keyword.fetch!(:internal_token)

    case get_req_header(conn, "x-internal-token") do
      [provided] ->
        if Plug.Crypto.secure_compare(provided, expected), do: conn, else: deny(conn)

      _missing ->
        deny(conn)
    end
  end

  defp deny(conn) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(401, ~s({"error":"unauthorized"}))
    |> halt()
  end
end
