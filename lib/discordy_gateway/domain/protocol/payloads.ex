defmodule DiscordyGateway.Domain.Protocol.Payloads do
  @moduledoc "Decoders for the `d` payload of every client-to-gateway opcode (P7)."

  import DiscordyGateway.Domain.Protocol.Validate

  @statuses ~w(online idle dnd invisible)
  @content_types ~w(application commit welcome proposal)
  @signal_kinds ~w(offer answer ice bye)
  @watch_actions ~w(load play pause seek)
  @max_recipients 1_000
  @max_ack_ids 1_000

  @spec decode(atom(), term()) :: {:ok, term()} | {:error, term()}
  def decode(:heartbeat, seq) when is_integer(seq) or is_nil(seq), do: {:ok, seq}
  def decode(command, d) when is_map(d), do: decode_map(command, d)
  def decode(_command, _d), do: {:error, :invalid_payload}

  defp decode_map(:identify, d) do
    with {:ok, token} <- string(d, "token"),
         {:ok, device_id} <- optional_string(d, "device_id"),
         {:ok, intents} <- intents(d),
         {:ok, properties} <- properties(d),
         {:ok, presence} <- optional_presence(d) do
      {:ok,
       %{
         token: token,
         device_id: device_id,
         intents: intents,
         properties: properties,
         presence: presence
       }}
    end
  end

  defp decode_map(:presence_update, d), do: presence(d)

  defp decode_map(:voice_state_update, d) do
    with {:ok, guild_id} <- optional_string(d, "guild_id"),
         {:ok, channel_id} <- optional_string(d, "channel_id"),
         {:ok, self_mute} <- boolean(d, "self_mute", :required),
         {:ok, self_deaf} <- boolean(d, "self_deaf", :required),
         {:ok, self_video} <- boolean(d, "self_video", false),
         {:ok, self_stream} <- boolean(d, "self_stream", false) do
      {:ok,
       %{
         guild_id: guild_id,
         channel_id: channel_id,
         self_mute: self_mute,
         self_deaf: self_deaf,
         self_video: self_video,
         self_stream: self_stream
       }}
    end
  end

  defp decode_map(:resume, d) do
    with {:ok, token} <- string(d, "token"),
         {:ok, session_id} <- string(d, "session_id"),
         {:ok, seq} <- non_neg_integer(d, "seq") do
      {:ok, %{token: token, session_id: session_id, seq: seq}}
    end
  end

  defp decode_map(:request_guild_members, d) do
    with {:ok, guild_id} <- string(d, "guild_id"),
         {:ok, query} <- optional_string(d, "query"),
         {:ok, limit} <- non_neg_integer(d, "limit"),
         {:ok, user_ids} <- string_list(d, "user_ids", :optional),
         {:ok, nonce} <- optional_string(d, "nonce"),
         {:ok, presences} <- boolean(d, "presences", false),
         :ok <- require_query_or_ids(query, user_ids) do
      {:ok,
       %{
         guild_id: guild_id,
         query: query,
         limit: limit,
         user_ids: user_ids,
         nonce: nonce,
         presences: presences
       }}
    end
  end

  defp decode_map(:e2ee_envelope, d) do
    with {:ok, channel_id} <- string(d, "channel_id"),
         {:ok, message_id} <- string(d, "message_id"),
         {:ok, sender_device_id} <- string(d, "sender_device_id"),
         {:ok, recipients} <- recipients(d),
         {:ok, mls_epoch} <- non_neg_integer(d, "mls_epoch"),
         {:ok, content_type} <- one_of(d, "content_type", @content_types) do
      {:ok,
       %{
         channel_id: channel_id,
         message_id: message_id,
         sender_device_id: sender_device_id,
         recipients: recipients,
         mls_epoch: mls_epoch,
         content_type: content_type
       }}
    end
  end

  defp decode_map(:e2ee_ack, d) do
    with {:ok, ids} <- string_list(d, "message_ids", :required),
         :ok <- max_length(ids, @max_ack_ids) do
      {:ok, %{message_ids: ids}}
    end
  end

  defp decode_map(:p2p_signal, d) do
    with {:ok, channel_id} <- string(d, "channel_id"),
         {:ok, to_user_id} <- string(d, "to_user_id"),
         {:ok, to_device_id} <- string(d, "to_device_id"),
         {:ok, from_user_id} <- string(d, "from_user_id"),
         {:ok, from_device_id} <- string(d, "from_device_id"),
         {:ok, kind} <- one_of(d, "kind", @signal_kinds),
         {:ok, payload} <- optional_string(d, "payload"),
         {:ok, signature} <- string(d, "identity_sig_b64"),
         :ok <- base64(signature) do
      {:ok,
       %{
         channel_id: channel_id,
         to_user_id: to_user_id,
         to_device_id: to_device_id,
         from_user_id: from_user_id,
         from_device_id: from_device_id,
         kind: kind,
         payload: payload || "",
         identity_sig_b64: signature
       }}
    end
  end

  defp decode_map(:voice_telemetry, d) do
    with {:ok, channel_id} <- string(d, "channel_id"),
         {:ok, jitter} <- non_neg_number(d, "jitter_ms"),
         {:ok, loss} <- non_neg_number(d, "loss_pct"),
         {:ok, rtt} <- non_neg_number(d, "rtt_ms"),
         {:ok, bitrate} <- non_neg_number(d, "bitrate_kbps") do
      {:ok,
       %{
         channel_id: channel_id,
         jitter_ms: jitter,
         loss_pct: loss,
         rtt_ms: rtt,
         bitrate_kbps: bitrate
       }}
    end
  end

  defp decode_map(:watch_together, d) do
    with {:ok, channel_id} <- string(d, "channel_id"),
         {:ok, action} <- one_of(d, "action", @watch_actions),
         {:ok, url} <- optional_string(d, "url"),
         :ok <- require_url(action, url),
         {:ok, position} <- non_neg_integer(d, "position_ms") do
      {:ok, %{channel_id: channel_id, action: action, url: url, position_ms: position}}
    end
  end

  defp decode_map(:call_decline, d) do
    with {:ok, channel_id} <- string(d, "channel_id") do
      {:ok, %{channel_id: channel_id}}
    end
  end

  defp intents(d) do
    case Map.get(d, "intents", 0) do
      intents when is_integer(intents) -> {:ok, intents}
      _ -> {:error, :invalid_intents}
    end
  end

  defp properties(d) do
    case Map.get(d, "properties") do
      %{} = p ->
        {:ok,
         %{
           os: string_or_empty(p, "os"),
           client: string_or_empty(p, "client"),
           version: string_or_empty(p, "version")
         }}

      nil ->
        {:ok, %{os: "", client: "", version: ""}}

      _ ->
        {:error, {:invalid, "properties"}}
    end
  end

  defp string_or_empty(map, key) do
    case Map.get(map, key) do
      value when is_binary(value) -> value
      _ -> ""
    end
  end

  defp optional_presence(d) do
    case Map.get(d, "presence") do
      nil -> {:ok, nil}
      %{} = presence -> presence(presence)
      _ -> {:error, {:invalid, "presence"}}
    end
  end

  defp presence(d) do
    with {:ok, status} <- one_of(d, "status", @statuses),
         {:ok, afk} <- boolean(d, "afk", false),
         {:ok, activities} <- activities(d) do
      {:ok, %{status: status, activities: activities, afk: afk}}
    end
  end

  defp activities(d) do
    case Map.get(d, "activities", []) do
      list when is_list(list) ->
        if Enum.all?(list, &is_map/1), do: {:ok, list}, else: {:error, {:invalid, "activities"}}

      _ ->
        {:error, {:invalid, "activities"}}
    end
  end

  defp recipients(d) do
    case Map.get(d, "recipients") do
      [_ | _] = list when length(list) <= @max_recipients -> collect_recipients(list)
      _ -> {:error, {:invalid, "recipients"}}
    end
  end

  defp collect_recipients(list) do
    Enum.reduce_while(list, {:ok, []}, fn entry, {:ok, acc} ->
      case recipient(entry) do
        {:ok, recipient} -> {:cont, {:ok, [recipient | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  defp recipient(%{} = entry) do
    with {:ok, user_id} <- string(entry, "user_id"),
         {:ok, device_id} <- string(entry, "device_id"),
         {:ok, ciphertext} <- string(entry, "ciphertext_b64"),
         :ok <- base64(ciphertext) do
      {:ok, %{user_id: user_id, device_id: device_id, ciphertext_b64: ciphertext}}
    end
  end

  defp recipient(_entry), do: {:error, {:invalid, "recipients"}}

  defp require_query_or_ids(nil, nil), do: {:error, :query_or_user_ids_required}
  defp require_query_or_ids(_query, _ids), do: :ok

  defp require_url("load", nil), do: {:error, :url_required}
  defp require_url(_action, _url), do: :ok

  defp max_length(list, max) when length(list) <= max, do: :ok
  defp max_length(_list, _max), do: {:error, :too_many}
end
