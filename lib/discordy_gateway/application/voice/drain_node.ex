defmodule DiscordyGateway.Application.Voice.DrainNode do
  @moduledoc "Drain and undrain use cases: stop new allocations on a node, and tell the node itself."

  alias DiscordyGateway.Application.Ports

  @spec drain(String.t()) :: :ok | {:error, term()}
  def drain(node_id) do
    with :ok <- Ports.voice_allocations().drain(node_id) do
      Ports.voice_node_control().drain(node_id)
    end
  end

  @spec undrain(String.t()) :: :ok | {:error, term()}
  def undrain(node_id) do
    with :ok <- Ports.voice_allocations().undrain(node_id) do
      Ports.voice_node_control().undrain(node_id)
    end
  end
end
