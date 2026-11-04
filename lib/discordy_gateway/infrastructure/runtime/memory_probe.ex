defmodule DiscordyGateway.Infrastructure.Runtime.MemoryProbe do
  @moduledoc """
  Memory figures of the running node. Usage is the VM's allocated memory (`:erlang.memory(:total)`),
  which falls again when sessions end, unlike the resident set the VM keeps after a peak. The limit
  is `MEMORY_LIMIT_BYTES` when set, else the container's cgroup limit (v2, then v1), else unknown.
  """

  import Bitwise

  @behaviour DiscordyGateway.Application.Ports.MemoryProbe

  @cgroup_files ["/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"]
  # cgroup v1 reports an effectively unlimited container as a value near 2^63.
  @unlimited 1 <<< 60

  @impl true
  def used_bytes, do: :erlang.memory(:total)

  @impl true
  def limit_bytes do
    case Application.get_env(:discordy_gateway, :settings, [])[:memory_limit_bytes] do
      limit when is_integer(limit) -> limit
      nil -> cgroup_limit(@cgroup_files)
    end
  end

  @doc false
  @spec parse_limit(String.t()) :: pos_integer() | nil
  def parse_limit(text) do
    case Integer.parse(String.trim(text)) do
      {bytes, ""} when bytes > 0 and bytes < @unlimited -> bytes
      _ -> nil
    end
  end

  defp cgroup_limit([]), do: nil

  defp cgroup_limit([file | rest]) do
    with {:ok, text} <- File.read(file),
         limit when is_integer(limit) <- parse_limit(text) do
      limit
    else
      _ -> cgroup_limit(rest)
    end
  end
end
