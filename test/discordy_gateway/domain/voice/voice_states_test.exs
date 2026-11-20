defmodule DiscordyGateway.Domain.Voice.VoiceStatesTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Voice.VoiceStates

  defp req(channel_id, opts \\ []) do
    %{
      channel_id: channel_id,
      self_mute: Keyword.get(opts, :self_mute, false),
      self_deaf: false,
      self_video: false,
      self_stream: false
    }
  end

  defp fresh, do: VoiceStates.new()

  test "first join is :joined and records the session" do
    {change, states} = VoiceStates.update(fresh(), "u1", "s1", req("c1"))
    assert change == :joined
    assert VoiceStates.get(states, "u1").channel_id == "c1"
    assert VoiceStates.get(states, "u1").session_id == "s1"
  end

  test "changing channel is :moved, same channel is :updated" do
    {_, states} = VoiceStates.update(fresh(), "u1", "s1", req("c1"))
    assert {:moved, states} = VoiceStates.update(states, "u1", "s1", req("c2"))
    assert {:updated, states} = VoiceStates.update(states, "u1", "s1", req("c2", self_mute: true))
    assert VoiceStates.get(states, "u1").self_mute
  end

  test "a null channel leaves; leaving when absent is a no-op" do
    {_, states} = VoiceStates.update(fresh(), "u1", "s1", req("c1"))
    assert {:left, states} = VoiceStates.update(states, "u1", "s1", req(nil))
    assert VoiceStates.empty?(states)
    assert {:noop, ^states} = VoiceStates.update(states, "u1", "s1", req(nil))
  end

  test "joining from another session takes over the user's state" do
    {_, states} = VoiceStates.update(fresh(), "u1", "s1", req("c1"))
    {change, states} = VoiceStates.update(states, "u1", "s2", req("c1"))
    assert change == :joined
    assert VoiceStates.get(states, "u1").session_id == "s2"
  end

  test "remove_session/2 drops the states owned by a session" do
    {_, states} = VoiceStates.update(fresh(), "u1", "s1", req("c1"))
    {_, states} = VoiceStates.update(states, "u2", "s2", req("c1"))
    assert {[%{user_id: "u1"}], states} = VoiceStates.remove_session(states, "s1")
    assert VoiceStates.user_ids(states) == ["u2"]
  end

  test "in_channel/2 lists the states of one channel" do
    {_, states} = VoiceStates.update(fresh(), "u1", "s1", req("c1"))
    {_, states} = VoiceStates.update(states, "u2", "s2", req("c2"))
    assert [%{user_id: "u1"}] = VoiceStates.in_channel(states, "c1")
  end

  test "to_payload/2 renders a Discord VoiceState" do
    {_, states} = VoiceStates.update(fresh(), "u1", "s1", req("c1"))
    payload = VoiceStates.to_payload(VoiceStates.get(states, "u1"), "g1")

    assert payload == %{
             "guild_id" => "g1",
             "channel_id" => "c1",
             "user_id" => "u1",
             "session_id" => "s1",
             "deaf" => false,
             "mute" => false,
             "self_deaf" => false,
             "self_mute" => false,
             "self_video" => false,
             "self_stream" => false,
             "suppress" => false,
             "request_to_speak_timestamp" => nil
           }

    left = VoiceStates.left_payload(VoiceStates.get(states, "u1"), "g1")
    assert left["channel_id"] == nil
    assert left["user_id"] == "u1"
  end

  test "the E2EE flag is recorded per user and survives updates in the same channel" do
    {_, states} = VoiceStates.update(fresh(), "u1", "s1", req("c1"))
    assert VoiceStates.get(states, "u1").e2ee == false

    states = VoiceStates.set_e2ee(states, "u1", true)
    assert {:updated, states} = VoiceStates.update(states, "u1", "s1", req("c1", self_mute: true))
    assert VoiceStates.get(states, "u1").e2ee == true
  end

  test "guarded stage joins start suppressed and cannot self-promote" do
    request = Map.put(req("stage"), :guarded_publish, true)
    {_, states} = VoiceStates.update(fresh(), "u1", "s1", request)
    assert %{guarded_publish: true, suppress: true} = VoiceStates.get(states, "u1")
    assert VoiceStates.to_payload(VoiceStates.get(states, "u1"), "g1")["suppress"]
    {_, states} = VoiceStates.update(states, "u1", "s1", Map.put(request, :suppress, false))
    assert VoiceStates.get(states, "u1").suppress
    {_, states} = VoiceStates.update(states, "u1", "s1", req("ordinary"))
    refute VoiceStates.get(states, "u1").suppress
  end

  test "an audience hand raise survives ordinary state updates and promotion clears it" do
    request =
      req("stage")
      |> Map.put(:guarded_publish, true)
      |> Map.put(:request_to_speak, "2026-11-20T01:00:00Z")

    {_, states} = VoiceStates.update(fresh(), "u1", "s1", request)
    assert VoiceStates.get(states, "u1").request_to_speak_timestamp == "2026-11-20T01:00:00Z"

    {_, states} =
      VoiceStates.update(states, "u1", "s1", Map.put(request, :request_to_speak, :unchanged))

    assert VoiceStates.get(states, "u1").request_to_speak_timestamp == "2026-11-20T01:00:00Z"
    states = VoiceStates.set_suppress(states, "u1", false)
    assert %{suppress: false, request_to_speak_timestamp: nil} = VoiceStates.get(states, "u1")
  end

  test "server-authorized stage host can start as a speaker" do
    request = req("stage") |> Map.put(:guarded_publish, true) |> Map.put(:stage_host, true)
    {_, states} = VoiceStates.update(fresh(), "host", "s1", request)
    refute VoiceStates.get(states, "host").suppress
    states = VoiceStates.set_suppress(states, "host", true)
    assert VoiceStates.get(states, "host").suppress
  end
end
