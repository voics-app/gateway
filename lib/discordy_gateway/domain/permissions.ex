defmodule DiscordyGateway.Domain.Permissions do
  @moduledoc """
  Discord's permission algorithm (P9), ported for gateway-side filtering:

  1. Start from @everyone's base permissions and OR in the member's roles.
  2. ADMINISTRATOR, or being the guild owner, grants everything.
  3. Apply channel overwrites: @everyone, then roles (all denies, then all allows), then member.
  4. Implicit rules: no VIEW_CHANNEL removes every channel permission; no SEND_MESSAGES in a text
     channel removes the permissions that depend on it; timed-out members keep only VIEW_CHANNEL
     and READ_MESSAGE_HISTORY.
  """

  import Bitwise

  alias DiscordyGateway.Contracts

  @all Contracts.permissions() |> Map.values() |> Enum.reduce(0, &bor/2)
  @administrator Contracts.permission(:administrator)
  @view_channel Contracts.permission(:view_channel)
  @send_messages Contracts.permission(:send_messages)
  @timeout_allowed @view_channel ||| Contracts.permission(:read_message_history)
  @send_dependent Contracts.permission(:mention_everyone) ||| Contracts.permission(:send_tts_messages) |||
                    Contracts.permission(:attach_files) ||| Contracts.permission(:embed_links)
  @text_channel_types [0, 1, 3, 5, 10, 11, 12, 15]

  @type overwrite :: %{id: String.t(), type: 0 | 1, allow: non_neg_integer(), deny: non_neg_integer()}
  @type member :: %{user_id: String.t(), role_ids: [String.t()]}
  @type channel_context :: %{
          owner_id: String.t(),
          guild_id: String.t(),
          role_permissions: %{String.t() => non_neg_integer()},
          member: member(),
          overwrites: [overwrite()],
          channel_type: integer(),
          timed_out: boolean()
        }

  @doc "Every known permission bit."
  @spec all() :: non_neg_integer()
  def all, do: @all

  @doc "Parses a permission value serialized as a decimal string (or already an integer)."
  @spec parse(String.t() | integer() | nil) :: non_neg_integer()
  def parse(nil), do: 0
  def parse(value) when is_integer(value), do: value

  def parse(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> 0
    end
  end

  @doc "Whether `perms` contains the named permission."
  @spec has?(non_neg_integer(), atom()) :: boolean()
  def has?(perms, name), do: (perms &&& Contracts.permission(name)) != 0

  @doc "Guild-level permissions (steps 1 and 2)."
  @spec compute_base(String.t(), String.t(), %{String.t() => non_neg_integer()}, member()) ::
          non_neg_integer()
  def compute_base(owner_id, _guild_id, _role_permissions, %{user_id: owner_id}), do: @all

  def compute_base(_owner_id, guild_id, role_permissions, member) do
    everyone = Map.get(role_permissions, guild_id, 0)

    perms =
      Enum.reduce(member.role_ids, everyone, fn role_id, acc ->
        acc ||| Map.get(role_permissions, role_id, 0)
      end)

    if (perms &&& @administrator) != 0, do: @all, else: perms
  end

  @doc "Applies channel overwrites to base permissions (step 3)."
  @spec compute_overwrites(non_neg_integer(), [overwrite()], member(), String.t()) :: non_neg_integer()
  def compute_overwrites(base, _overwrites, _member, _guild_id) when (base &&& @administrator) != 0,
    do: @all

  def compute_overwrites(base, overwrites, member, guild_id) do
    base
    |> apply_everyone(overwrites, guild_id)
    |> apply_roles(overwrites, member.role_ids)
    |> apply_member(overwrites, member.user_id)
  end

  @doc "Full channel permission computation including the implicit rules (steps 1-4)."
  @spec compute_channel(channel_context()) :: non_neg_integer()
  def compute_channel(ctx) do
    base = compute_base(ctx.owner_id, ctx.guild_id, ctx.role_permissions, ctx.member)

    if base == @all do
      @all
    else
      base
      |> compute_overwrites(ctx.overwrites, ctx.member, ctx.guild_id)
      |> apply_timeout(ctx.timed_out)
      |> apply_implicit(ctx.channel_type)
    end
  end

  defp apply_everyone(perms, overwrites, guild_id) do
    case Enum.find(overwrites, &(&1.type == 0 and &1.id == guild_id)) do
      nil -> perms
      ow -> apply_overwrite(perms, ow.allow, ow.deny)
    end
  end

  defp apply_roles(perms, overwrites, role_ids) do
    {allow, deny} =
      overwrites
      |> Enum.filter(&(&1.type == 0 and &1.id in role_ids))
      |> Enum.reduce({0, 0}, fn ow, {allow, deny} -> {allow ||| ow.allow, deny ||| ow.deny} end)

    apply_overwrite(perms, allow, deny)
  end

  defp apply_member(perms, overwrites, user_id) do
    case Enum.find(overwrites, &(&1.type == 1 and &1.id == user_id)) do
      nil -> perms
      ow -> apply_overwrite(perms, ow.allow, ow.deny)
    end
  end

  defp apply_overwrite(perms, allow, deny), do: (perms &&& ~~~deny) ||| allow

  defp apply_timeout(perms, true), do: perms &&& @timeout_allowed
  defp apply_timeout(perms, false), do: perms

  defp apply_implicit(perms, _type) when (perms &&& @view_channel) == 0, do: 0

  defp apply_implicit(perms, type) when type in @text_channel_types and (perms &&& @send_messages) == 0,
    do: perms &&& ~~~@send_dependent

  defp apply_implicit(perms, _type), do: perms
end
