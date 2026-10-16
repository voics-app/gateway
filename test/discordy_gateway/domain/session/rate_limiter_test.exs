defmodule DiscordyGateway.Domain.Session.RateLimiterTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Session.RateLimiter

  test "allows up to the limit inside a window" do
    limiter = RateLimiter.new(3, 1_000)
    assert {:ok, limiter} = RateLimiter.hit(limiter, 0)
    assert {:ok, limiter} = RateLimiter.hit(limiter, 10)
    assert {:ok, limiter} = RateLimiter.hit(limiter, 20)
    assert RateLimiter.hit(limiter, 30) == :rate_limited
  end

  test "slides: old hits expire after the window" do
    limiter = RateLimiter.new(2, 1_000)
    {:ok, limiter} = RateLimiter.hit(limiter, 0)
    {:ok, limiter} = RateLimiter.hit(limiter, 500)
    assert RateLimiter.hit(limiter, 999) == :rate_limited
    assert {:ok, limiter} = RateLimiter.hit(limiter, 1_000)
    assert RateLimiter.hit(limiter, 1_200) == :rate_limited
    assert {:ok, _} = RateLimiter.hit(limiter, 1_500)
  end

  test "P7 defaults are 120 sends per 60 seconds" do
    limiter = RateLimiter.p7()
    limiter = Enum.reduce(1..120, limiter, fn i, acc -> elem(RateLimiter.hit(acc, i), 1) end)
    assert RateLimiter.hit(limiter, 121) == :rate_limited
  end
end
