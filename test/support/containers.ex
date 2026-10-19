defmodule DiscordyGateway.Containers do
  @moduledoc """
  Throwaway Docker containers for integration tests (`mix test --include integration`).
  Containers are named `gateway-it-*`, publish to random host ports, and are removed on exit.
  """

  @doc "Runs `image` with `args`, waits for `ready_log` in its logs, and returns `{name, host_port}`."
  def start(image, container_port, args \\ [], ready_log) do
    name = "gateway-it-#{System.unique_integer([:positive])}"

    {_, 0} =
      System.cmd(
        "docker",
        ["run", "-d", "--name", name, "-p", "127.0.0.1::#{container_port}"] ++ [image | args]
      )

    ExUnit.Callbacks.on_exit(fn ->
      System.cmd("docker", ["rm", "-f", name], stderr_to_stdout: true)
    end)

    await_log(name, ready_log, 60)
    {name, host_port(name, container_port)}
  end

  defp host_port(name, container_port) do
    {out, 0} = System.cmd("docker", ["port", name, "#{container_port}/tcp"])

    out
    |> String.split("\n", trim: true)
    |> hd()
    |> String.split(":")
    |> List.last()
    |> String.to_integer()
  end

  defp await_log(_name, _pattern, 0), do: raise("container never became ready")

  defp await_log(name, pattern, attempts) do
    {logs, _} = System.cmd("docker", ["logs", name], stderr_to_stdout: true)

    unless String.contains?(logs, pattern) do
      Process.sleep(500)
      await_log(name, pattern, attempts - 1)
    end
  end
end
