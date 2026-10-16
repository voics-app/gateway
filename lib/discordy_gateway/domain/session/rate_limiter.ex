defmodule DiscordyGateway.Domain.Session.RateLimiter do
  @moduledoc "Sliding-window send limiter for client frames (P7: 120 sends per 60 s)."

  alias DiscordyGateway.Contracts

  @enforce_keys [:limit, :window_ms]
  defstruct [:limit, :window_ms, hits: :queue.new(), count: 0]

  @type t :: %__MODULE__{}

  @spec new(pos_integer(), pos_integer()) :: t()
  def new(limit, window_ms), do: %__MODULE__{limit: limit, window_ms: window_ms}

  @doc "Limiter configured with the P7 budget."
  @spec p7() :: t()
  def p7, do: new(Contracts.rate_limit_sends(), Contracts.rate_limit_window_ms())

  @doc "Records a send at `now_ms`, or reports that the budget is exhausted."
  @spec hit(t(), integer()) :: {:ok, t()} | :rate_limited
  def hit(%__MODULE__{} = limiter, now_ms) do
    limiter = expire(limiter, now_ms - limiter.window_ms)

    if limiter.count >= limiter.limit do
      :rate_limited
    else
      {:ok, %{limiter | hits: :queue.in(now_ms, limiter.hits), count: limiter.count + 1}}
    end
  end

  defp expire(limiter, threshold) do
    case :queue.peek(limiter.hits) do
      {:value, at} when at <= threshold ->
        expire(%{limiter | hits: :queue.drop(limiter.hits), count: limiter.count - 1}, threshold)

      _ ->
        limiter
    end
  end
end
