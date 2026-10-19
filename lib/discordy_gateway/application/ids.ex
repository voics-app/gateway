defmodule DiscordyGateway.Application.Ids do
  @moduledoc "Identifier generation: UUIDv4 session ids and UUIDv7 event ids (P6)."

  import Bitwise

  @spec uuid_v4() :: String.t()
  def uuid_v4 do
    <<a::48, _::4, b::12, _::2, c::62>> = :crypto.strong_rand_bytes(16)
    format(<<a::48, 4::4, b::12, 2::2, c::62>>)
  end

  @spec uuid_v7(integer()) :: String.t()
  def uuid_v7(now_ms \\ System.os_time(:millisecond)) do
    <<rand_a::12, _::2, rand_b::62, _::4>> = :crypto.strong_rand_bytes(10)
    ts = now_ms &&& 0xFFFFFFFFFFFF
    format(<<ts::48, 7::4, rand_a::12, 2::2, rand_b::62>>)
  end

  defp format(<<a::32, b::16, c::16, d::16, e::48>>) do
    [{a, 8}, {b, 4}, {c, 4}, {d, 4}, {e, 12}]
    |> Enum.map_join("-", fn {value, width} ->
      value |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(width, "0")
    end)
  end
end
