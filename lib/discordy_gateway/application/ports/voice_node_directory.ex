defmodule DiscordyGateway.Application.Ports.VoiceNodeDirectory do
  @moduledoc "Live view of the voice nodes registered in etcd (P8)."

  alias DiscordyGateway.Domain.Voice.VoiceNode

  @callback list_nodes() :: [VoiceNode.t()]
  @callback get_node(node_id :: String.t()) :: {:ok, VoiceNode.t()} | :error
end
