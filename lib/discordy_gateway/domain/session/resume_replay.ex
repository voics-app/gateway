defmodule DiscordyGateway.Domain.Session.ResumeReplay do
  @moduledoc """
  Decides what a RESUME replays from the buffered dispatches: everything after the client's last
  seen sequence, provided the buffer still covers it without a gap.
  """

  @type entry :: %{required(:seq) => non_neg_integer(), optional(atom()) => term()}

  @spec select([entry()], non_neg_integer(), non_neg_integer()) ::
          {:ok, [entry()]} | {:error, :invalid_seq | :gap}
  def select(_buffer, client_seq, current_seq) when client_seq > current_seq, do: {:error, :invalid_seq}
  def select(_buffer, seq, seq), do: {:ok, []}

  def select(buffer, client_seq, _current_seq) do
    sorted = Enum.sort_by(buffer, & &1.seq)

    case sorted do
      [%{seq: oldest} | _] when oldest <= client_seq + 1 ->
        {:ok, Enum.filter(sorted, &(&1.seq > client_seq))}

      _ ->
        {:error, :gap}
    end
  end
end
