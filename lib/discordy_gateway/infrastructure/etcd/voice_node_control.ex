defmodule DiscordyGateway.Infrastructure.Etcd.VoiceNodeControl do
  @moduledoc "Signals drain/undrain to a voice node through etcd key `/discordy/voice/drain/<node_id>`."

  @behaviour DiscordyGateway.Application.Ports.VoiceNodeControl

  alias DiscordyGateway.Infrastructure.Etcd.VoiceNodeDirectory

  @prefix "/discordy/voice/drain/"

  @impl true
  def drain(node_id) do
    case :eetcd_kv.put(VoiceNodeDirectory.connection(), @prefix <> node_id, "1") do
      {:ok, _response} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def undrain(node_id) do
    case :eetcd_kv.delete(VoiceNodeDirectory.connection(), @prefix <> node_id) do
      {:ok, _response} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
