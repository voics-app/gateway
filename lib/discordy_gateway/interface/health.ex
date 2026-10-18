defmodule DiscordyGateway.Interface.Health do
  @moduledoc """
  Readiness: runs the dependency checks bound by the composition root under the `:readiness`
  application key, as `{name, {module, function}}` pairs whose function returns a boolean.
  """

  @spec readiness() :: {:ok, map()} | {:error, map()}
  def readiness do
    checks =
      :discordy_gateway
      |> Application.get_env(:readiness, [])
      |> Map.new(fn {name, {module, function}} ->
        {Atom.to_string(name), run(module, function)}
      end)

    if Enum.all?(checks, fn {_name, ok?} -> ok? end), do: {:ok, checks}, else: {:error, checks}
  end

  defp run(module, function) do
    apply(module, function, []) == true
  catch
    _kind, _reason -> false
  end
end
