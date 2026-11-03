defmodule DiscordyGateway.Domain.Activities do
  @moduledoc """
  Sanitises the activities a client sends in a presence update before they are stored and
  fanned out to everyone who can see the user. Unknown fields are dropped; a payload that
  breaks a limit or has a wrongly typed field is rejected as a whole.
  """

  @custom_status_type 4
  @allowed_types [0, 1, 2, 3, 4, 5]
  @max_activities 5
  @max_text 128
  @max_url 512
  @text_fields ~w(name state details)

  @spec sanitize(term()) :: {:ok, [map()]} | {:error, :invalid_activities}
  def sanitize(list) when is_list(list) and length(list) <= @max_activities do
    with {:ok, activities} <- collect(list),
         true <- Enum.count(activities, &(&1["type"] == @custom_status_type)) <= 1 do
      {:ok, activities}
    else
      _ -> {:error, :invalid_activities}
    end
  end

  def sanitize(_other), do: {:error, :invalid_activities}

  defp collect(list) do
    Enum.reduce_while(list, {:ok, []}, fn entry, {:ok, acc} ->
      case activity(entry) do
        {:ok, activity} -> {:cont, {:ok, [activity | acc]}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      :error -> :error
    end
  end

  defp activity(%{"name" => name, "type" => type} = raw)
       when is_binary(name) and type in @allowed_types do
    Enum.reduce_while(
      [&texts/2, &url/2, &emoji/2, &created_at/2, &timestamps/2],
      {:ok, %{"name" => name, "type" => type}},
      fn step, {:ok, acc} ->
        case step.(raw, acc) do
          {:ok, acc} -> {:cont, {:ok, acc}}
          :error -> {:halt, :error}
        end
      end
    )
    |> case do
      {:ok, %{"name" => name} = clean} when byte_size(name) > 0 -> {:ok, clean}
      _ -> :error
    end
  end

  defp activity(_other), do: :error

  defp texts(raw, acc) do
    Enum.reduce_while(@text_fields, {:ok, acc}, fn key, {:ok, acc} ->
      case Map.get(raw, key) do
        nil -> {:cont, {:ok, acc}}
        text when is_binary(text) -> halt_on_error(bounded(acc, key, text, @max_text))
        _ -> {:halt, :error}
      end
    end)
  end

  defp url(raw, acc) do
    case Map.get(raw, "url") do
      nil -> {:ok, acc}
      url when is_binary(url) -> bounded(acc, "url", url, @max_url)
      _ -> :error
    end
  end

  defp emoji(raw, acc) do
    case Map.get(raw, "emoji") do
      nil -> {:ok, acc}
      %{} = emoji -> emoji_fields(emoji, acc)
      _ -> :error
    end
  end

  defp emoji_fields(emoji, acc) do
    id = Map.get(emoji, "id")
    name = Map.get(emoji, "name")
    animated = Map.get(emoji, "animated")

    if optional?(id, &is_binary/1) and byte_size_ok?(id) and optional?(name, &is_binary/1) and
         byte_size_ok?(name) and optional?(animated, &is_boolean/1) do
      clean = %{"id" => id, "name" => name, "animated" => animated}
      {:ok, Map.put(acc, "emoji", Map.reject(clean, fn {_k, v} -> is_nil(v) end))}
    else
      :error
    end
  end

  defp created_at(raw, acc) do
    case Map.get(raw, "created_at") do
      nil -> {:ok, acc}
      at when is_integer(at) and at >= 0 -> {:ok, Map.put(acc, "created_at", at)}
      _ -> :error
    end
  end

  defp timestamps(raw, acc) do
    case Map.get(raw, "timestamps") do
      nil -> {:ok, acc}
      %{} = ts -> timestamp_fields(ts, acc)
      _ -> :error
    end
  end

  defp timestamp_fields(ts, acc) do
    start = Map.get(ts, "start")
    finish = Map.get(ts, "end")

    if optional?(start, &non_neg_integer?/1) and optional?(finish, &non_neg_integer?/1) do
      clean = Map.reject(%{"start" => start, "end" => finish}, fn {_k, v} -> is_nil(v) end)
      {:ok, Map.put(acc, "timestamps", clean)}
    else
      :error
    end
  end

  defp bounded(acc, key, text, max) do
    if String.length(text) <= max, do: {:ok, Map.put(acc, key, text)}, else: :error
  end

  defp halt_on_error({:ok, _acc} = ok), do: {:cont, ok}
  defp halt_on_error(:error), do: {:halt, :error}

  defp optional?(nil, _check), do: true
  defp optional?(value, check), do: check.(value)

  defp byte_size_ok?(value) when is_binary(value), do: String.length(value) <= @max_text
  defp byte_size_ok?(_nil), do: true

  defp non_neg_integer?(value), do: is_integer(value) and value >= 0
end
