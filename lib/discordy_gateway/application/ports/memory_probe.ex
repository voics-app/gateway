defmodule DiscordyGateway.Application.Ports.MemoryProbe do
  @moduledoc "Memory in use by the gateway and the limit it runs under."

  @callback used_bytes() :: non_neg_integer()
  @doc "The memory limit in bytes, or `nil` when none is known (admission control is then off)."
  @callback limit_bytes() :: pos_integer() | nil
end
