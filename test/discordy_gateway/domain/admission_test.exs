defmodule DiscordyGateway.Domain.AdmissionTest do
  use ExUnit.Case, async: true

  alias DiscordyGateway.Domain.Admission

  @limit 1_000

  test "stays open without a known limit, whatever the usage" do
    assert Admission.decide(:open, 10_000_000, nil, 0.75) == :open
    assert Admission.decide(:closed, 10_000_000, nil, 0.75) == :open
  end

  test "an open gate closes once usage reaches the fraction of the limit" do
    assert Admission.decide(:open, 749, @limit, 0.75) == :open
    assert Admission.decide(:open, 750, @limit, 0.75) == :closed
    assert Admission.decide(:open, 2_000, @limit, 0.75) == :closed
  end

  test "a closed gate reopens only once usage is 5 points below the fraction" do
    assert Admission.decide(:closed, 740, @limit, 0.75) == :closed
    assert Admission.decide(:closed, 700, @limit, 0.75) == :closed
    assert Admission.decide(:closed, 699, @limit, 0.75) == :open
  end
end
