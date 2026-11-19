defmodule DiscordyGateway.Fakes do
  @moduledoc "In-memory port implementations for tests, backed by one public ETS table."

  @table :discordy_fakes

  def start do
    if :ets.whereis(@table) == :undefined, do: :ets.new(@table, [:named_table, :public, :set])
    :ok
  end

  def reset do
    :ets.delete_all_objects(@table)
    :ok
  end

  def put(key, value), do: :ets.insert(@table, {key, value})

  def get(key, default \\ nil) do
    case :ets.lookup(@table, key) do
      [{^key, value}] -> value
      [] -> default
    end
  end

  def delete(key), do: :ets.delete(@table, key)

  def update(key, default, fun), do: put(key, fun.(get(key, default)))

  def keys(kind), do: for({{^kind, id}, _value} <- :ets.tab2list(@table), do: id)

  def calls(name), do: Enum.reverse(get({:calls, name}, []))
  def record(name, call), do: update({:calls, name}, [], &[call | &1])
end

defmodule DiscordyGateway.Fakes.TokenVerifier do
  @moduledoc false
  @behaviour DiscordyGateway.Application.Ports.TokenVerifier

  @impl true
  def verify_access_token("user-" <> id),
    do: {:ok, %{user_id: id, auth_session_id: "auth-" <> id, device_id: "dev-" <> id}}

  def verify_access_token(_token), do: {:error, :invalid}
end

defmodule DiscordyGateway.Fakes.ApiClient do
  @moduledoc false
  @behaviour DiscordyGateway.Application.Ports.ApiClient

  alias DiscordyGateway.Fakes

  @impl true
  def introspect("good-bot"),
    do:
      {:ok,
       %{"user_id" => "900", "kind" => "bot", "application_id" => "900", "session_id" => nil}}

  def introspect("limited-bot"),
    do:
      {:ok,
       %{"user_id" => "901", "kind" => "bot", "application_id" => "901", "approved_intents" => 0}}

  def introspect(_token), do: {:error, :unauthorized}

  @impl true
  def fetch_ready(user_id) do
    Fakes.get(
      {:ready, user_id},
      {:ok,
       %{
         "user" => %{"id" => user_id},
         "guilds" => [],
         "private_channels" => [],
         "relationships" => [],
         "user_settings" => %{}
       }}
    )
  end

  @impl true
  def fetch_guild_state(guild_id), do: Fakes.get({:guild, guild_id}, {:error, :not_found})

  @impl true
  def fetch_channel_access(channel_id, user_id),
    do: Fakes.get({:access, channel_id, user_id}, {:error, :forbidden})

  @impl true
  def voice_activity_checkpoint(checkpoint) do
    Fakes.record(:voice_checkpoint, checkpoint)

    if observer = Fakes.get(:voice_checkpoint_observer),
      do: send(observer, {:voice_checkpoint, checkpoint})

    Fakes.get(:voice_checkpoint_result, :ok)
  end
end

defmodule DiscordyGateway.Fakes.ResumeStore do
  @moduledoc false
  @behaviour DiscordyGateway.Application.Ports.ResumeStore

  alias DiscordyGateway.Fakes

  @impl true
  def append(session_id, entries) do
    Fakes.update({:resume, session_id}, [], &Enum.take(&1 ++ entries, -1000))
    :ok
  end

  @impl true
  def fetch(session_id), do: {:ok, Fakes.get({:resume, session_id}, [])}

  @impl true
  def delete(session_id) do
    Fakes.put({:resume, session_id}, [])
    :ok
  end
end

defmodule DiscordyGateway.Fakes.PresenceStore do
  @moduledoc false
  @behaviour DiscordyGateway.Application.Ports.PresenceStore

  alias DiscordyGateway.Domain.Presence
  alias DiscordyGateway.Fakes

  @impl true
  def put(user_id, session_id, presence) do
    Fakes.update({:presence, user_id}, %{}, &Map.put(&1, session_id, presence))
    {:ok, Map.values(Fakes.get({:presence, user_id}))}
  end

  @impl true
  def remove(user_id, session_id) do
    Fakes.update({:presence, user_id}, %{}, &Map.delete(&1, session_id))
    {:ok, Map.values(Fakes.get({:presence, user_id}))}
  end

  @impl true
  def swap_aggregate(user_id, aggregate) do
    previous = Fakes.get({:aggregate, user_id})
    Fakes.put({:aggregate, user_id}, aggregate)
    if previous == aggregate, do: :unchanged, else: :changed
  end

  @impl true
  def get_aggregates(user_ids) do
    for id <- user_ids, into: %{} do
      sessions = Map.values(Fakes.get({:presence, id}, %{}))
      {id, Presence.aggregate(sessions)}
    end
  end

  @impl true
  def refresh(user_id, _session_id), do: {:ok, Map.values(Fakes.get({:presence, user_id}, %{}))}
end

defmodule DiscordyGateway.Fakes.InboxStore do
  @moduledoc false
  @behaviour DiscordyGateway.Application.Ports.InboxStore

  alias DiscordyGateway.Fakes

  @impl true
  def store(device_id, message_id, json, expires_at_ms) do
    Fakes.update({:inbox, device_id}, [], &(&1 ++ [{message_id, json, expires_at_ms}]))
    :ok
  end

  @impl true
  def list(device_id),
    do: {:ok, for({id, json, _expires} <- Fakes.get({:inbox, device_id}, []), do: {id, json})}

  @impl true
  def ack(device_id, ids) do
    Fakes.update({:inbox, device_id}, [], fn entries ->
      Enum.reject(entries, fn {id, _, _} -> id in ids end)
    end)

    :ok
  end

  @impl true
  def sweep_expired(now_ms, _limit) do
    removed =
      Enum.reduce(Fakes.keys(:inbox), 0, fn device_id, acc ->
        {expired, kept} =
          {:inbox, device_id}
          |> Fakes.get([])
          |> Enum.split_with(fn {_id, _json, expires} -> expires <= now_ms end)

        Fakes.put({:inbox, device_id}, kept)
        acc + length(expired)
      end)

    {:ok, removed}
  end
end

defmodule DiscordyGateway.Fakes.EventPublisher do
  @moduledoc false
  @behaviour DiscordyGateway.Application.Ports.EventPublisher

  alias DiscordyGateway.Fakes

  @impl true
  def publish(topic, key, envelope) do
    Fakes.record(:published, {topic, key, envelope})
    :ok
  end
end

defmodule DiscordyGateway.Fakes.VoiceNodes do
  @moduledoc false
  @behaviour DiscordyGateway.Application.Ports.VoiceNodeDirectory

  alias DiscordyGateway.Fakes

  @impl true
  def list_nodes, do: Fakes.get(:voice_nodes, [])

  @impl true
  def get_node(node_id) do
    case Enum.find(list_nodes(), &(&1.node_id == node_id)) do
      nil -> :error
      node -> {:ok, node}
    end
  end
end

defmodule DiscordyGateway.Fakes.VoiceAllocations do
  @moduledoc false
  @behaviour DiscordyGateway.Application.Ports.VoiceAllocationStore

  alias DiscordyGateway.Fakes

  @impl true
  def get(server_id) do
    case Fakes.get({:alloc, server_id}) do
      nil -> :none
      node_id -> {:ok, node_id}
    end
  end

  @impl true
  def claim(server_id, node_id) do
    if Fakes.get({:alloc, server_id}) == nil, do: Fakes.put({:alloc, server_id}, node_id)
    {:ok, Fakes.get({:alloc, server_id})}
  end

  @impl true
  def release(server_id, node_id) do
    if Fakes.get({:alloc, server_id}) == node_id, do: Fakes.delete({:alloc, server_id})
    :ok
  end

  @impl true
  def count, do: length(Fakes.keys(:alloc))

  @impl true
  def drained, do: MapSet.new(Fakes.get(:drained, []))

  @impl true
  def drain(node_id) do
    Fakes.update(:drained, [], &[node_id | &1])
    :ok
  end

  @impl true
  def undrain(node_id) do
    Fakes.update(:drained, [], &List.delete(&1, node_id))
    :ok
  end
end

defmodule DiscordyGateway.Fakes.VoiceNodeControl do
  @moduledoc false
  @behaviour DiscordyGateway.Application.Ports.VoiceNodeControl

  alias DiscordyGateway.Fakes

  @impl true
  def drain(node_id) do
    Fakes.record(:control, {:drain, node_id})
    :ok
  end

  @impl true
  def undrain(node_id) do
    Fakes.record(:control, {:undrain, node_id})
    :ok
  end
end

defmodule DiscordyGateway.Fakes.VoiceTokenSigner do
  @moduledoc false
  @behaviour DiscordyGateway.Application.Ports.VoiceTokenSigner

  @impl true
  def sign(claims),
    do:
      "voice-token:" <> claims["sub"] <> ":" <> claims["node"] <> ":" <> to_string(claims["e2ee"])
end

defmodule DiscordyGateway.Fakes.TurnCredentials do
  @moduledoc false
  @behaviour DiscordyGateway.Application.Ports.TurnCredentials

  @impl true
  def issue(user_id, now_s),
    do: %{
      urls: ["turn:turn.test:3478"],
      username: "#{now_s + 3600}:#{user_id}",
      credential: "secret",
      ttl: 3600
    }
end

defmodule DiscordyGateway.Fakes.CallDefaults do
  @moduledoc false
  @behaviour DiscordyGateway.Application.Ports.CallDefaults

  alias DiscordyGateway.Fakes

  @impl true
  def put(user_id, enabled) do
    Fakes.put({:call_default, user_id}, enabled)
    :ok
  end

  @impl true
  def enabled_user_ids(user_ids) do
    Fakes.record(:call_defaults, user_ids)

    case Fakes.get(:call_defaults_failures, 0) do
      failures when failures > 0 ->
        Fakes.put(:call_defaults_failures, failures - 1)
        {:error, :unavailable}

      _healthy ->
        {:ok, Enum.filter(user_ids, &Fakes.get({:call_default, &1}, false))}
    end
  end
end

defmodule DiscordyGateway.Fakes.MessagesClient do
  @moduledoc false
  @behaviour DiscordyGateway.Application.Ports.MessagesClient

  alias DiscordyGateway.Fakes

  @impl true
  def fetch_read_states(user_id) do
    Fakes.record(:read_states, user_id)
    if hook = Fakes.get(:read_states_hook), do: hook.(user_id)
    Fakes.get({:read_states, user_id}, {:ok, %{entries: [], versioned: false}})
  end

  @impl true
  def create_call_message(channel_id, author_id, participants) do
    Fakes.record(:messages_client, {:create, channel_id, author_id, participants})

    if Fakes.get(:messages_client_failing, false),
      do: {:error, :unavailable},
      else: {:ok, "call-message-" <> channel_id}
  end

  @impl true
  def update_call_message(channel_id, message_id, update) do
    Fakes.record(:messages_client, {:update, channel_id, message_id, update})
    if Fakes.get(:messages_client_failing, false), do: {:error, :unavailable}, else: :ok
  end
end

defmodule DiscordyGateway.Fakes.MemoryProbe do
  @moduledoc """
  Memory probe whose reading a test sets with `put/2`. It reads the application environment, not
  the fakes table, because the admission process samples it while the application boots.
  """
  @behaviour DiscordyGateway.Application.Ports.MemoryProbe

  @doc "Sets the reading; a `nil` limit means no limit is known."
  def put(used, limit), do: Application.put_env(:discordy_gateway, :fake_memory, {used, limit})

  @doc "Back to an unknown limit."
  def reset, do: Application.delete_env(:discordy_gateway, :fake_memory)

  @impl true
  def used_bytes, do: elem(reading(), 0)

  @impl true
  def limit_bytes, do: elem(reading(), 1)

  defp reading, do: Application.get_env(:discordy_gateway, :fake_memory, {0, nil})
end
