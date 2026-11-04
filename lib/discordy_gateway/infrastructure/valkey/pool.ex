defmodule DiscordyGateway.Infrastructure.Valkey.Pool do
  @moduledoc """
  A small fixed pool of Redix connections to Valkey. Redix pipelines concurrent callers over one
  connection, so a handful of connections is plenty; callers are spread round-robin.

  Bulk writes get their own connections: Redix answers in order, so a short command behind a
  large batch on the same connection waits for the whole batch, so presence and other short
  calls must not share a connection with resume buffer writes.
  """

  use Supervisor

  alias DiscordyGateway.Infrastructure.Valkey.ResumeWriter

  @size 4
  @counter :valkey_pool_counter
  @names List.to_tuple(for index <- 0..(@size - 1), do: :"valkey_#{index}")
  @bulk_size 2
  @bulk_names List.to_tuple(for index <- 0..(@bulk_size - 1), do: :"valkey_bulk_#{index}")

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    url = Keyword.fetch!(opts, :url)
    :persistent_term.put(@counter, :counters.new(1, []))

    connections = Tuple.to_list(@names) ++ Tuple.to_list(@bulk_names)

    children =
      for name <- connections do
        Supervisor.child_spec({Redix, {redis_url(url), [name: name, sync_connect: false]}},
          id: {Redix, name}
        )
      end

    Supervisor.init(children ++ ResumeWriter.child_specs(), strategy: :one_for_one)
  end

  @spec command([String.t() | integer()]) :: {:ok, term()} | {:error, term()}
  def command(command), do: Redix.command(connection(), stringify(command))

  @spec pipeline([[String.t() | integer()]]) :: {:ok, [term()]} | {:error, term()}
  def pipeline(commands), do: Redix.pipeline(connection(), Enum.map(commands, &stringify/1))

  @doc "Like `pipeline/1`, on the connections kept apart for large batches."
  @spec bulk_pipeline([[String.t() | integer()]], non_neg_integer()) ::
          {:ok, [term()]} | {:error, term()}
  def bulk_pipeline(commands, shard) do
    Redix.pipeline(elem(@bulk_names, rem(shard, @bulk_size)), Enum.map(commands, &stringify/1))
  end

  @doc "Whether Valkey answers PING."
  @spec ready?() :: boolean()
  def ready?, do: match?({:ok, "PONG"}, command(["PING"]))

  defp connection do
    counter = :persistent_term.get(@counter)
    :counters.add(counter, 1, 1)
    name(rem(:counters.get(counter, 1), @size))
  end

  defp name(index), do: elem(@names, index)

  defp stringify(command), do: Enum.map(command, &to_string/1)

  defp redis_url("valkey://" <> rest), do: "redis://" <> rest
  defp redis_url("valkeys://" <> rest), do: "rediss://" <> rest
  defp redis_url(url), do: url
end
