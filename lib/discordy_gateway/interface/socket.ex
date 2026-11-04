defmodule DiscordyGateway.Interface.Socket do
  @moduledoc """
  WebSocket transport for `/gateway`. It owns no protocol state: inbound text frames are parsed
  as JSON and handed to the session process; frames and close requests the session sends back
  are written to the socket.
  """

  @behaviour WebSock

  alias DiscordyGateway.Application.{Admission, Session}
  alias DiscordyGateway.Contracts

  @impl true
  def init(_opts) do
    if Admission.admit?() do
      start_session()
    else
      {:stop, :normal, {Contracts.close_code(:server_overloaded), ""}, %{session: nil}}
    end
  end

  defp start_session do
    case Session.start(self()) do
      {:ok, session} ->
        {:ok, %{session: session}}

      {:error, _reason} ->
        {:stop, :normal, {Contracts.close_code(:unknown_error), ""}, %{session: nil}}
    end
  end

  @impl true
  def handle_in({text, [opcode: :text]}, state) do
    case Jason.decode(text) do
      {:ok, frame} ->
        Session.frame(state.session, frame)
        {:ok, state}

      {:error, _reason} ->
        {:stop, :normal, {Contracts.close_code(:decode_error), ""}, state}
    end
  end

  def handle_in({_binary, [opcode: :binary]}, state),
    do: {:stop, :normal, {Contracts.close_code(:decode_error), ""}, state}

  @impl true
  def handle_info({:gw_frame, frame}, state), do: {:push, {:text, frame}, state}
  def handle_info({:gw_close, code}, state), do: {:stop, :normal, {code, ""}, state}
  def handle_info(_message, state), do: {:ok, state}

  @impl true
  def terminate(_reason, _state), do: :ok
end
