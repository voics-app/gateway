defmodule DiscordyGateway.Application.RoutingReadiness do
  @moduledoc "Fences admission and event acknowledgements while cluster routing is unavailable."

  @spec checks() :: map()
  def checks do
    :discordy_gateway
    |> Application.get_env(:routing_readiness, [])
    |> Map.new(fn {name, {module, function}} ->
      {Atom.to_string(name), check(module, function)}
    end)
  end

  @spec ready?() :: boolean()
  def ready?, do: Enum.all?(checks(), fn {_name, ready} -> ready end)

  @doc "Waits without acknowledging the current record; brod heartbeats run in another process."
  @spec await_ready() :: :ok
  def await_ready do
    if ready?() do
      :ok
    else
      Process.sleep(100)
      await_ready()
    end
  end

  defp check(module, function) do
    apply(module, function, []) == true
  catch
    _kind, _reason -> false
  end
end
