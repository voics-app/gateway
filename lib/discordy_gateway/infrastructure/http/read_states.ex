defmodule DiscordyGateway.Infrastructure.Http.ReadStates do
  @moduledoc "Validates the messages read-state array and its explicit authority certification."

  @max_version 9_223_372_036_854_775_807
  @max_snowflake 18_446_744_073_709_551_615
  @max_count 4_294_967_295

  def decode(entries, header) when is_list(entries) and header in [[], ["1"]] do
    versioned = header == ["1"]

    if Enum.all?(entries, &valid_entry?(&1, versioned)) and unique_channels?(entries),
      do: {:ok, %{entries: entries, versioned: versioned}},
      else: {:error, :invalid_read_states}
  end

  def decode(_entries, _header), do: {:error, :invalid_read_states}

  defp valid_entry?(
         %{"id" => id, "last_message_id" => marker, "mention_count" => count} = row,
         versioned
       ) do
    snowflake?(id) and (is_nil(marker) or snowflake?(marker)) and
      is_integer(count) and count >= 0 and count <= @max_count and valid_version?(row, versioned)
  end

  defp valid_entry?(_entry, _versioned), do: false

  defp valid_version?(%{"read_state_version" => version}, _versioned) when is_binary(version) do
    Regex.match?(~r/^(0|[1-9][0-9]{0,18})$/, version) and
      String.to_integer(version) <= @max_version
  end

  defp valid_version?(row, false), do: not Map.has_key?(row, "read_state_version")
  defp valid_version?(_row, _versioned), do: false

  defp snowflake?(id) when is_binary(id),
    do: Regex.match?(~r/^[0-9]{1,20}$/, id) and String.to_integer(id) <= @max_snowflake

  defp snowflake?(_id), do: false

  defp unique_channels?(entries),
    do: MapSet.size(MapSet.new(entries, & &1["id"])) == length(entries)
end
