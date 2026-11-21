defmodule DiscordyGateway.Domain.Voice.Capabilities do
  @moduledoc """
  Gateway-side preparation of a node-affine, revisioned publication grant.

  This is a protocol building block only. It does not send or enforce a grant; a caller must
  disconnect or otherwise fail closed on timeout, rejection, expired lease, or a missing ACK.
  """

  alias DiscordyGateway.Domain.Permissions

  @max_ttl_ms 15_000

  @enforce_keys [:session_id, :node_id, :registration_lease]
  defstruct [:session_id, :node_id, :registration_lease, revision: 0, pending: nil, applied: nil]

  @type t :: %__MODULE__{}

  @doc "A new session starts without an acknowledged grant."
  @spec new(String.t(), String.t(), pos_integer()) :: t()
  def new(session_id, node_id, registration_lease)
      when is_binary(session_id) and is_binary(node_id) and is_integer(registration_lease) and
             registration_lease > 0 do
    %__MODULE__{session_id: session_id, node_id: node_id, registration_lease: registration_lease}
  end

  @doc "Compute all capabilities together from effective channel permissions."
  @spec permissions(non_neg_integer(), boolean()) :: map()
  def permissions(perms, suppressed?) do
    connected? = Permissions.has?(perms, :view_channel) and Permissions.has?(perms, :connect)
    audio? = connected? and not suppressed? and Permissions.has?(perms, :speak)
    video? = connected? and not suppressed? and Permissions.has?(perms, :stream)
    soundboard? = audio? and Permissions.has?(perms, :use_soundboard)

    %{
      "can_publish_audio" => audio?,
      "can_publish_video" => video?,
      "can_publish_soundboard" => soundboard?
    }
  end

  @doc "Prepare exactly one in-flight update with a maximum 15-second expiry."
  @spec prepare(t(), non_neg_integer(), boolean(), integer(), pos_integer()) ::
          {:ok, t(), map()} | {:error, :pending | :invalid_ttl}
  def prepare(%__MODULE__{pending: pending}, _perms, _suppressed?, _now_ms, _ttl_ms)
      when not is_nil(pending),
      do: {:error, :pending}

  def prepare(%__MODULE__{} = state, perms, suppressed?, now_ms, ttl_ms)
      when is_integer(ttl_ms) and ttl_ms > 0 and ttl_ms <= @max_ttl_ms do
    revision = state.revision + 1

    command =
      permissions(perms, suppressed?)
      |> Map.merge(%{
        "session_id" => state.session_id,
        "node_id" => state.node_id,
        "registration_lease" => state.registration_lease,
        "revision" => revision,
        "expires_at" => now_ms + ttl_ms
      })

    {:ok, %{state | revision: revision, pending: command}, command}
  end

  def prepare(%__MODULE__{}, _perms, _suppressed?, _now_ms, _ttl_ms),
    do: {:error, :invalid_ttl}

  @doc "Only a matching ACK of the exact pending command marks it applied."
  @spec acknowledge(t(), map(), integer()) ::
          {:ok, t()} | {:error, :mismatch | :expired, t(), :disconnect}
  def acknowledge(%__MODULE__{pending: pending} = state, ack, now_ms) when is_map(pending) do
    cond do
      ack != pending -> rejected(state, :mismatch)
      now_ms >= pending["expires_at"] -> rejected(state, :expired)
      true -> {:ok, %{state | pending: nil, applied: pending}}
    end
  end

  def acknowledge(%__MODULE__{} = state, _ack, _now_ms), do: rejected(state, :mismatch)

  defp rejected(state, reason) do
    {state, :disconnect} = fail(state)
    {:error, reason, state, :disconnect}
  end

  @doc "After a failed or uncertain update the caller must disconnect; no grant is considered safe."
  @spec fail(t()) :: {t(), :disconnect}
  def fail(%__MODULE__{} = state), do: {%{state | pending: nil, applied: nil}, :disconnect}

  @doc "An absent or elapsed acknowledged grant must be treated as denied."
  @spec expired?(t(), integer()) :: boolean()
  def expired?(%__MODULE__{applied: %{"expires_at" => expires}}, now), do: now >= expires
  def expired?(%__MODULE__{}, _now), do: true

  @doc "Renew before the supplied margin; pending or absent grants are never considered current."
  @spec renew_due?(t(), integer(), non_neg_integer()) :: boolean()
  def renew_due?(%__MODULE__{pending: nil, applied: %{"expires_at" => expires}}, now, margin),
    do: now + margin >= expires

  def renew_due?(%__MODULE__{}, _now, _margin), do: true
end
