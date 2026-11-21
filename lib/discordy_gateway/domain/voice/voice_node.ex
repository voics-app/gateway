defmodule DiscordyGateway.Domain.Voice.VoiceNode do
  @moduledoc "A voice node as registered in etcd (P8), and least-loaded selection."

  @enforce_keys [:node_id, :region, :ws_url]
  defstruct [
    :node_id,
    :region,
    :ws_url,
    :public_ip,
    :version,
    :control_endpoint,
    udp_port: 50_000,
    users: 0,
    cpu_pct: 0.0,
    egress_bps: 0,
    draining: false,
    probe_required: false,
    health: :unknown,
    blacklisted: false
  ]

  @type t :: %__MODULE__{}

  @spec parse(map()) :: {:ok, t()} | {:error, :invalid_node}
  def parse(%{"probe_required" => value}) when not is_boolean(value),
    do: {:error, :invalid_node}

  def parse(%{"node_id" => id, "region" => region, "ws_url" => ws_url} = raw)
      when is_binary(id) and is_binary(region) and is_binary(ws_url) do
    {:ok,
     %__MODULE__{
       node_id: id,
       region: region,
       ws_url: ws_url,
       public_ip: raw["public_ip"],
       version: raw["version"],
       control_endpoint: control_endpoint(raw),
       udp_port: number(raw["udp_port"], 50_000),
       users: number(raw["users"], 0),
       cpu_pct: number(raw["cpu_pct"], 0.0),
       egress_bps: number(raw["egress_bps"], 0),
       draining: raw["draining"] == true,
       probe_required: raw["probe_required"] == true
     }}
  end

  def parse(_raw), do: {:error, :invalid_node}

  # A control address is optional and cannot be derived from the public websocket URL.
  # The etcd registration is trusted, but reject public addresses and incomplete lease identities.
  defp control_endpoint(%{
         "control_ip" => ip,
         "control_port" => port,
         "registration_lease" => lease
       })
       when is_binary(ip) and is_integer(port) and port in 1..65_535 and is_binary(lease) do
    with {number, ""} when number > 0 <- Integer.parse(lease),
         {:ok, address} <- :inet.parse_address(String.to_charlist(ip)),
         true <- private_ipv4?(address) do
      %{ip: ip, port: port, lease: lease}
    else
      _ -> nil
    end
  end

  defp control_endpoint(_raw), do: nil

  @doc "Returns a validated private control endpoint, if this registration has one."
  @spec validated_control(t()) :: map() | nil
  def validated_control(%__MODULE__{control_endpoint: %{ip: ip, port: port, lease: lease}}),
    do:
      control_endpoint(%{
        "control_ip" => ip,
        "control_port" => port,
        "registration_lease" => lease
      })

  def validated_control(_node), do: nil
  defp private_ipv4?({10, _, _, _}), do: true
  defp private_ipv4?({172, b, _, _}) when b in 16..31, do: true
  defp private_ipv4?({192, 168, _, _}), do: true
  defp private_ipv4?(_), do: false

  @doc "Rejects ambiguous node identities and endpoints; signaling and media require affinity."
  @spec unique_routes([t()]) :: [t()]
  def unique_routes(nodes) do
    counts = nodes |> Enum.flat_map(&routes/1) |> Enum.frequencies()
    Enum.filter(nodes, fn node -> Enum.all?(routes(node), &(counts[&1] == 1)) end)
  end

  defp routes(node) do
    uri = URI.parse(node.ws_url)

    socket =
      {uri.scheme, uri.host, uri.port, String.trim_trailing(uri.path || "", "/"), uri.query}

    routes = [{:node, node.node_id}, {:socket, socket}]
    if node.public_ip, do: [{:media, node.public_ip, node.udp_port} | routes], else: routes
  end

  @doc "Whether the node accepts new allocations (not draining in etcd nor via the gateway)."
  @spec available?(t(), MapSet.t()) :: boolean()
  def available?(%__MODULE__{} = node, drained_ids),
    do: healthy?(node) and not node.draining and not MapSet.member?(drained_ids, node.node_id)

  @doc "Allocation health, independently of registration and existing-call drain policy."
  @spec healthy?(t()) :: boolean()
  def healthy?(%__MODULE__{blacklisted: true}), do: false
  def healthy?(%__MODULE__{health: :unhealthy}), do: false
  def healthy?(%__MODULE__{probe_required: true, health: status}), do: status == :healthy
  def healthy?(%__MODULE__{}), do: true

  @doc "P8 step 2: lowest `users` in the region, ties broken by `cpu_pct` then id."
  @spec pick_least_loaded([t()], String.t(), MapSet.t()) :: {:ok, t()} | {:error, :no_voice_nodes}
  def pick_least_loaded(nodes, region, drained_ids) do
    nodes
    |> Enum.filter(&(&1.region == region and available?(&1, drained_ids)))
    |> Enum.min_by(&{&1.users, &1.cpu_pct, &1.node_id}, fn -> nil end)
    |> case do
      nil -> {:error, :no_voice_nodes}
      node -> {:ok, node}
    end
  end

  defp number(value, _default) when is_number(value), do: value
  defp number(_value, default), do: default
end
