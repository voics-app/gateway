defmodule DiscordyGateway.Application.Ports.VoiceAllocationStore do
  @moduledoc "Voice server allocations (`voice:alloc:<srv_id>`) and gateway-side drain marks."

  @callback get(server_id :: String.t()) :: {:ok, String.t()} | :none
  @doc "Stores `node_id` unless an allocation exists; returns the allocation that won."
  @callback claim(server_id :: String.t(), node_id :: String.t()) ::
              {:ok, String.t()} | {:error, term()}
  @doc "Deletes the allocation only if it still points at `node_id`."
  @callback release(server_id :: String.t(), node_id :: String.t()) :: :ok
  @callback count() :: non_neg_integer()
  @callback drained() :: MapSet.t()
  @callback drain(node_id :: String.t()) :: :ok | {:error, term()}
  @callback undrain(node_id :: String.t()) :: :ok | {:error, term()}
end
