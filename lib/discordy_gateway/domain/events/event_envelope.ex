defmodule DiscordyGateway.Domain.Events.EventEnvelope do
  @moduledoc "The P6 event-bus envelope and the gateway's routing rule for it."

  @enforce_keys [:id, :type, :data]
  defstruct [:id, :type, :occurred_at, :guild_id, :channel_id, :target_user_ids, :data, v: 1]

  @type t :: %__MODULE__{
          id: String.t(),
          type: String.t(),
          v: pos_integer(),
          occurred_at: String.t() | nil,
          guild_id: String.t() | nil,
          channel_id: String.t() | nil,
          target_user_ids: [String.t()] | nil,
          data: map()
        }

  @type route :: {:guild, String.t()} | {:users, [String.t()]} | :drop

  @spec parse(term()) :: {:ok, t()} | {:error, :invalid_envelope}
  def parse(%{"id" => id, "type" => type, "data" => data} = raw)
      when is_binary(id) and is_binary(type) and is_map(data) do
    {:ok,
     %__MODULE__{
       id: id,
       type: type,
       v: Map.get(raw, "v", 1),
       occurred_at: raw["occurred_at"],
       guild_id: raw["guild_id"],
       channel_id: raw["channel_id"],
       target_user_ids: targets(raw["target_user_ids"]),
       data: data
     }}
  end

  def parse(_raw), do: {:error, :invalid_envelope}

  @doc """
  P6 routing: a guild id without targets fans out through the guild process; otherwise the event
  goes to every session of each targeted user. Envelopes with neither are dropped.
  """
  @spec route(t()) :: route()
  def route(%__MODULE__{target_user_ids: [_ | _] = ids}), do: {:users, ids}
  def route(%__MODULE__{guild_id: guild_id}) when is_binary(guild_id), do: {:guild, guild_id}
  def route(%__MODULE__{}), do: :drop

  @doc "Builds an envelope to publish. `id` and `occurred_at` are supplied by the caller."
  @spec new(String.t(), map(), keyword()) :: t()
  def new(type, data, opts) do
    %__MODULE__{
      id: Keyword.fetch!(opts, :id),
      type: type,
      occurred_at: Keyword.fetch!(opts, :occurred_at),
      guild_id: Keyword.get(opts, :guild_id),
      channel_id: Keyword.get(opts, :channel_id),
      target_user_ids: Keyword.get(opts, :target_user_ids),
      data: data
    }
  end

  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = env) do
    %{
      "id" => env.id,
      "type" => env.type,
      "v" => env.v,
      "occurred_at" => env.occurred_at,
      "guild_id" => env.guild_id,
      "channel_id" => env.channel_id,
      "target_user_ids" => env.target_user_ids,
      "data" => env.data
    }
  end

  defp targets(ids) when is_list(ids), do: Enum.filter(ids, &is_binary/1)
  defp targets(_ids), do: nil
end
