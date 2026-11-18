defmodule DiscordyGateway.Interface.Health do
  @moduledoc """
  Readiness: runs the dependency checks bound by the composition root under the `:readiness`
  application key, as `{name, {module, function}}` pairs whose function returns a boolean.

  Checks under `:readiness_soft` are reported in the body but never gate readiness: the gateway
  keeps serving without them and only the feature behind them degrades (etcd: voice allocation).
  """

  alias DiscordyGateway.Application.RoutingReadiness

  @spec readiness() :: {:ok, map()} | {:error, map()}
  def readiness do
    hard = Map.merge(run_all(:readiness), RoutingReadiness.checks())
    checks = Map.merge(run_all(:readiness_soft), hard)

    if Enum.all?(hard, fn {_name, ok?} -> ok? end), do: {:ok, checks}, else: {:error, checks}
  end

  defp run_all(key) do
    :discordy_gateway
    |> Application.get_env(key, [])
    |> Map.new(fn {name, {module, function}} -> {Atom.to_string(name), run(module, function)} end)
  end

  defp run(module, function) do
    apply(module, function, []) == true
  catch
    _kind, _reason -> false
  end
end
