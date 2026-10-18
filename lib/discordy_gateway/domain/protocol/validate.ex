defmodule DiscordyGateway.Domain.Protocol.Validate do
  @moduledoc "Small field validators shared by the payload decoders."

  @spec string(map(), String.t()) :: {:ok, String.t()} | {:error, {:invalid, String.t()}}
  def string(map, key) do
    case Map.get(map, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:invalid, key}}
    end
  end

  @spec optional_string(map(), String.t()) ::
          {:ok, String.t() | nil} | {:error, {:invalid, String.t()}}
  def optional_string(map, key) do
    case Map.get(map, key) do
      nil -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, {:invalid, key}}
    end
  end

  @spec boolean(map(), String.t(), boolean() | :required) ::
          {:ok, boolean()} | {:error, {:invalid, String.t()}}
  def boolean(map, key, default) do
    case Map.get(map, key) do
      value when is_boolean(value) -> {:ok, value}
      nil when is_boolean(default) -> {:ok, default}
      _ -> {:error, {:invalid, key}}
    end
  end

  @spec non_neg_integer(map(), String.t()) ::
          {:ok, non_neg_integer()} | {:error, {:invalid, String.t()}}
  def non_neg_integer(map, key) do
    case Map.get(map, key) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, {:invalid, key}}
    end
  end

  @spec non_neg_number(map(), String.t()) :: {:ok, number()} | {:error, {:invalid, String.t()}}
  def non_neg_number(map, key) do
    case Map.get(map, key) do
      value when is_number(value) and value >= 0 -> {:ok, value}
      _ -> {:error, {:invalid, key}}
    end
  end

  @spec one_of(map(), String.t(), [String.t()]) ::
          {:ok, String.t()} | {:error, {:invalid, String.t()}}
  def one_of(map, key, allowed) do
    value = Map.get(map, key)
    if value in allowed, do: {:ok, value}, else: {:error, {:invalid, key}}
  end

  @spec string_list(map(), String.t(), :required | :optional) ::
          {:ok, [String.t()] | nil} | {:error, {:invalid, String.t()}}
  def string_list(map, key, presence) do
    case {Map.get(map, key), presence} do
      {nil, :optional} ->
        {:ok, nil}

      {list, _} when is_list(list) and list != [] ->
        if Enum.all?(list, &is_binary/1), do: {:ok, list}, else: {:error, {:invalid, key}}

      _ ->
        {:error, {:invalid, key}}
    end
  end

  @spec base64(String.t()) :: :ok | {:error, :invalid_base64}
  def base64(value) do
    case Base.decode64(value, padding: false) do
      {:ok, _} -> :ok
      :error -> {:error, :invalid_base64}
    end
  end
end
