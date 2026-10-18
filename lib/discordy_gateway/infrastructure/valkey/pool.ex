defmodule DiscordyGateway.Infrastructure.Valkey.Pool do
  @moduledoc """
  A small fixed pool of Redix connections to Valkey. Redix pipelines concurrent callers over one
  connection, so a handful of connections is plenty; callers are spread round-robin.
  """

  use Supervisor

  @size 4
  @counter :valkey_pool_counter

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    url = Keyword.fetch!(opts, :url)
    :persistent_term.put(@counter, :counters.new(1, []))

    children =
      for index <- 0..(@size - 1) do
        Supervisor.child_spec({Redix, {redis_url(url), [name: name(index), sync_connect: false]}},
          id: {Redix, index}
        )
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec command([String.t() | integer()]) :: {:ok, term()} | {:error, term()}
  def command(command), do: Redix.command(connection(), stringify(command))

  @spec pipeline([[String.t() | integer()]]) :: {:ok, [term()]} | {:error, term()}
  def pipeline(commands), do: Redix.pipeline(connection(), Enum.map(commands, &stringify/1))

  @doc "Whether Valkey answers PING."
  @spec ready?() :: boolean()
  def ready?, do: match?({:ok, "PONG"}, command(["PING"]))

  defp connection do
    counter = :persistent_term.get(@counter)
    :counters.add(counter, 1, 1)
    name(rem(:counters.get(counter, 1), @size))
  end

  defp name(index), do: :"valkey_#{index}"

  defp stringify(command), do: Enum.map(command, &to_string/1)

  defp redis_url("valkey://" <> rest), do: "redis://" <> rest
  defp redis_url("valkeys://" <> rest), do: "rediss://" <> rest
  defp redis_url(url), do: url
end
