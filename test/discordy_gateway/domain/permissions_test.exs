defmodule DiscordyGateway.Domain.PermissionsTest do
  use ExUnit.Case, async: true
  import Bitwise

  alias DiscordyGateway.Domain.Permissions

  @guild "100"
  @owner "1"
  @user "2"
  @mod_role "300"
  @muted_role "301"

  @view 1 <<< 10
  @send 1 <<< 11
  @tts 1 <<< 12
  @embed 1 <<< 14
  @attach 1 <<< 15
  @history 1 <<< 16
  @mention 1 <<< 17
  @connect 1 <<< 20
  @admin 1 <<< 3
  @manage_channels 1 <<< 4
  @ban 1 <<< 2

  defp roles(extra \\ %{}), do: Map.merge(%{@guild => @view ||| @send ||| @history}, extra)
  defp member(role_ids \\ []), do: %{user_id: @user, role_ids: role_ids}

  describe "parse/1" do
    test "accepts decimal strings, integers and nil" do
      assert Permissions.parse("1024") == 1024
      assert Permissions.parse(2048) == 2048
      assert Permissions.parse(nil) == 0
    end
  end

  describe "compute_base/4 (guild-level, Discord step 1-2)" do
    test "the owner gets every permission" do
      assert Permissions.compute_base(@owner, @guild, roles(), %{user_id: @owner, role_ids: []}) ==
               Permissions.all()
    end

    test "@everyone base OR member roles" do
      base = Permissions.compute_base(@owner, @guild, roles(%{@mod_role => @ban}), member([@mod_role]))
      assert base == (@view ||| @send ||| @history ||| @ban)
    end

    test "roles the member does not hold are ignored" do
      base = Permissions.compute_base(@owner, @guild, roles(%{@mod_role => @ban}), member())
      assert (base &&& @ban) == 0
    end

    test "ADMINISTRATOR on any role grants everything" do
      base = Permissions.compute_base(@owner, @guild, roles(%{@mod_role => @admin}), member([@mod_role]))
      assert base == Permissions.all()
    end

    test "unknown role ids contribute nothing" do
      assert Permissions.compute_base(@owner, @guild, roles(), member(["999"])) ==
               (@view ||| @send ||| @history)
    end
  end

  describe "compute_overwrites/4 (Discord step 3)" do
    test "no overwrites keeps the base" do
      assert Permissions.compute_overwrites(@view ||| @send, [], member(), @guild) == (@view ||| @send)
    end

    test "@everyone overwrite denies then allows" do
      ow = [%{id: @guild, type: 0, allow: @connect, deny: @send}]
      assert Permissions.compute_overwrites(@view ||| @send, ow, member(), @guild) == (@view ||| @connect)
    end

    test "role overwrites: all denies are applied before all allows" do
      ow = [
        %{id: @mod_role, type: 0, allow: @send, deny: 0},
        %{id: @muted_role, type: 0, allow: 0, deny: @send}
      ]

      perms = Permissions.compute_overwrites(@view, ow, member([@mod_role, @muted_role]), @guild)
      assert (perms &&& @send) != 0
    end

    test "role overwrites only apply to roles the member holds" do
      ow = [%{id: @muted_role, type: 0, allow: 0, deny: @send}]
      assert Permissions.compute_overwrites(@view ||| @send, ow, member([@mod_role]), @guild) ==
               (@view ||| @send)
    end

    test "member overwrite wins over role overwrites" do
      ow = [
        %{id: @mod_role, type: 0, allow: @send, deny: 0},
        %{id: @user, type: 1, allow: 0, deny: @send}
      ]

      perms = Permissions.compute_overwrites(@view ||| @send, ow, member([@mod_role]), @guild)
      assert (perms &&& @send) == 0
    end

    test "@everyone deny of VIEW_CHANNEL can be re-allowed by a role" do
      ow = [
        %{id: @guild, type: 0, allow: 0, deny: @view},
        %{id: @mod_role, type: 0, allow: @view, deny: 0}
      ]

      assert (Permissions.compute_overwrites(@view, ow, member([@mod_role]), @guild) &&& @view) != 0
      assert (Permissions.compute_overwrites(@view, ow, member(), @guild) &&& @view) == 0
    end

    test "a role overwrite whose id is a user is not applied as a role" do
      ow = [%{id: @user, type: 0, allow: 0, deny: @send}]
      assert Permissions.compute_overwrites(@view ||| @send, ow, member(), @guild) == (@view ||| @send)
    end

    test "administrators ignore overwrites" do
      ow = [%{id: @guild, type: 0, allow: 0, deny: @view}]
      assert Permissions.compute_overwrites(Permissions.all(), ow, member(), @guild) == Permissions.all()
    end
  end

  describe "compute_channel/2 (full algorithm incl. implicit rules, step 4)" do
    defp ctx(overrides) do
      Map.merge(
        %{
          owner_id: @owner,
          guild_id: @guild,
          role_permissions: roles(),
          member: member(),
          overwrites: [],
          channel_type: 0,
          timed_out: false
        },
        overrides
      )
    end

    test "missing VIEW_CHANNEL removes all channel permissions" do
      ow = [%{id: @guild, type: 0, allow: 0, deny: @view}]
      assert Permissions.compute_channel(ctx(%{overwrites: ow})) == 0
    end

    test "missing SEND_MESSAGES in a text channel strips dependent permissions" do
      all_text = @view ||| @send ||| @tts ||| @embed ||| @attach ||| @mention
      ow = [%{id: @guild, type: 0, allow: 0, deny: @send}]
      perms = Permissions.compute_channel(ctx(%{role_permissions: %{@guild => all_text}, overwrites: ow}))
      assert perms == @view
    end

    test "voice channels keep embed/attach without SEND_MESSAGES" do
      base = @view ||| @connect ||| @attach
      perms = Permissions.compute_channel(ctx(%{role_permissions: %{@guild => base}, channel_type: 2}))
      assert perms == base
    end

    test "timed-out members keep only VIEW_CHANNEL and READ_MESSAGE_HISTORY" do
      assert Permissions.compute_channel(ctx(%{timed_out: true})) == (@view ||| @history)
    end

    test "timeouts do not affect administrators or the owner" do
      admin = ctx(%{timed_out: true, role_permissions: roles(%{@mod_role => @admin}), member: member([@mod_role])})
      assert Permissions.compute_channel(admin) == Permissions.all()

      owner = ctx(%{timed_out: true, member: %{user_id: @owner, role_ids: []}})
      assert Permissions.compute_channel(owner) == Permissions.all()
    end

    test "the owner sees channels that deny @everyone" do
      ow = [%{id: @guild, type: 0, allow: 0, deny: @view ||| @manage_channels}]
      owner = ctx(%{overwrites: ow, member: %{user_id: @owner, role_ids: []}})
      assert Permissions.compute_channel(owner) == Permissions.all()
    end
  end

  test "has?/2 checks a named permission" do
    assert Permissions.has?(@view, :view_channel)
    refute Permissions.has?(@send, :view_channel)
  end

  test "all/0 includes ADMINISTRATOR and USE_EXTERNAL_APPS" do
    assert (Permissions.all() &&& @admin) != 0
    assert (Permissions.all() &&& 1 <<< 50) != 0
  end
end
