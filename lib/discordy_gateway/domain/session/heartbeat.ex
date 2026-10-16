defmodule DiscordyGateway.Domain.Session.Heartbeat do
  @moduledoc """
  Heartbeat liveness. A connection is considered dead when no heartbeat arrived within one and a
  half intervals, which tolerates client jitter and network delay.
  """

  @enforce_keys [:interval_ms, :last_beat_ms]
  defstruct [:interval_ms, :last_beat_ms]

  @type t :: %__MODULE__{interval_ms: pos_integer(), last_beat_ms: integer()}

  @spec new(pos_integer(), integer()) :: t()
  def new(interval_ms, now_ms), do: %__MODULE__{interval_ms: interval_ms, last_beat_ms: now_ms}

  @spec beat(t(), integer()) :: t()
  def beat(%__MODULE__{} = hb, now_ms), do: %{hb | last_beat_ms: now_ms}

  @spec expired?(t(), integer()) :: boolean()
  def expired?(%__MODULE__{} = hb, now_ms), do: now_ms - hb.last_beat_ms > div(hb.interval_ms * 3, 2)

  @spec check_interval(t()) :: pos_integer()
  def check_interval(%__MODULE__{interval_ms: interval}), do: interval
end
