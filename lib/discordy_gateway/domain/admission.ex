defmodule DiscordyGateway.Domain.Admission do
  @moduledoc """
  Admission decision of the gateway: whether a new connection is accepted, given the memory in
  use and the memory limit. Connections are refused once usage reaches `fraction` of the limit and
  accepted again only after it has fallen `@hysteresis` below that, so the gate does not flap while
  memory hovers around the threshold.
  """

  @hysteresis 0.05

  @type t :: :open | :closed

  @doc """
  The next gate state. Without a limit (`nil`) the gate stays open.
  """
  @spec decide(t(), non_neg_integer(), pos_integer() | nil, float()) :: t()
  def decide(_current, _used, nil, _fraction), do: :open

  def decide(:open, used, limit, fraction),
    do: if(used >= limit * fraction, do: :closed, else: :open)

  def decide(:closed, used, limit, fraction),
    do: if(used < limit * (fraction - @hysteresis), do: :open, else: :closed)
end
