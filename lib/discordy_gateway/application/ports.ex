defmodule DiscordyGateway.Application.Ports do
  @moduledoc """
  Resolves port implementations. The composition root (or the test configuration) binds every
  port to an adapter under the `:ports` application environment key.
  """

  @spec token_verifier() :: module()
  def token_verifier, do: fetch(:token_verifier)
  @spec api_client() :: module()
  def api_client, do: fetch(:api_client)
  @spec messages_client() :: module()
  def messages_client, do: fetch(:messages_client)
  @spec resume_store() :: module()
  def resume_store, do: fetch(:resume_store)
  @spec presence_store() :: module()
  def presence_store, do: fetch(:presence_store)
  @spec call_defaults() :: module()
  def call_defaults, do: fetch(:call_defaults)
  @spec inbox_store() :: module()
  def inbox_store, do: fetch(:inbox_store)
  @spec event_publisher() :: module()
  def event_publisher, do: fetch(:event_publisher)
  @spec voice_nodes() :: module()
  def voice_nodes, do: fetch(:voice_nodes)
  @spec voice_allocations() :: module()
  def voice_allocations, do: fetch(:voice_allocations)
  @spec voice_node_control() :: module()
  def voice_node_control, do: fetch(:voice_node_control)
  @spec voice_token_signer() :: module()
  def voice_token_signer, do: fetch(:voice_token_signer)
  @spec session_directory() :: module()
  def session_directory, do: fetch(:session_directory)
  @spec turn_credentials() :: module()
  def turn_credentials, do: fetch(:turn_credentials)
  @spec memory_probe() :: module()
  def memory_probe, do: fetch(:memory_probe)
  @spec server_directory() :: module()
  def server_directory, do: fetch(:server_directory)

  defp fetch(port),
    do: :discordy_gateway |> Application.fetch_env!(:ports) |> Keyword.fetch!(port)
end
