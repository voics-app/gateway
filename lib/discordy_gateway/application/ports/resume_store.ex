defmodule DiscordyGateway.Application.Ports.ResumeStore do
  @moduledoc "Buffer of dispatches a disconnected session missed (P7 `gw:resume:<session_id>`)."

  @type entry :: {seq :: non_neg_integer(), frame :: binary()}

  @callback append(session_id :: String.t(), [entry()]) :: :ok | {:error, term()}
  @callback fetch(session_id :: String.t()) :: {:ok, [entry()]} | {:error, term()}
  @callback delete(session_id :: String.t()) :: :ok | {:error, term()}
end
