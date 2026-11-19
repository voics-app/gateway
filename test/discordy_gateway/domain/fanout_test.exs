defmodule DiscordyGateway.Domain.FanoutTest do
  use ExUnit.Case, async: true
  import Bitwise

  alias DiscordyGateway.Domain.{Fanout, GuildState, Intents, Subscriber}
  alias DiscordyGateway.Fixtures

  @all Intents.all()

  defp sub(id, user_id, opts \\ []) do
    %Subscriber{
      pid: id,
      user_id: user_id,
      intents: Keyword.get(opts, :intents, @all),
      bot: Keyword.get(opts, :bot, false),
      application_id: Keyword.get(opts, :application_id)
    }
  end

  setup do
    state = GuildState.from_payload(Fixtures.guild_payload())
    subs = [sub(:owner, "1"), sub(:alice, "2"), sub(:bob, "3")]
    {:ok, state: state, subs: subs}
  end

  defp targets(plan), do: plan |> Enum.map(&elem(&1, 0)) |> Enum.sort()

  test "channel events reach only members with VIEW_CHANNEL", %{state: state, subs: subs} do
    plan =
      Fanout.plan(
        state,
        subs,
        "MESSAGE_CREATE",
        %{"channel_id" => "11", "content" => "x"},
        "11",
        0
      )

    assert targets(plan) == [:alice, :owner]

    plan = Fanout.plan(state, subs, "MESSAGE_CREATE", %{"channel_id" => "10"}, "10", 0)
    assert targets(plan) == [:alice, :bob, :owner]
  end

  test "channel id falls back to the payload when the envelope has none", %{
    state: state,
    subs: subs
  } do
    plan = Fanout.plan(state, subs, "TYPING_START", %{"channel_id" => "11"}, nil, 0)
    assert targets(plan) == [:alice, :owner]
  end

  test "CHANNEL_* events use the channel's own id", %{state: state, subs: subs} do
    plan =
      Fanout.plan(state, subs, "CHANNEL_UPDATE", %{"id" => "11", "guild_id" => "100"}, nil, 0)

    assert targets(plan) == [:alice, :owner]
  end

  test "thread events stop reaching subscribers after the parent is removed", %{
    state: state,
    subs: subs
  } do
    for type <- [11, 12] do
      thread = %{"id" => "21", "type" => type, "parent_id" => "10", "owner_id" => "2"}

      state =
        state
        |> GuildState.apply_event("THREAD_CREATE", thread)
        |> GuildState.apply_event("CHANNEL_DELETE", %{"id" => "10"})

      assert Fanout.plan(state, subs, "THREAD_UPDATE", thread, nil, 0) == []
      assert Fanout.plan(state, subs, "MESSAGE_CREATE", %{"channel_id" => "21"}, "21", 0) == []
    end
  end

  test "private thread events and messages are hidden from other members", %{
    state: state,
    subs: subs
  } do
    thread = %{"id" => "21", "type" => 12, "parent_id" => "10", "owner_id" => "2"}
    state = GuildState.apply_event(state, "THREAD_CREATE", thread)

    for event <- ~w(THREAD_CREATE THREAD_UPDATE THREAD_DELETE) do
      assert targets(Fanout.plan(state, subs, event, thread, nil, 0)) == [:alice, :owner]
    end

    assert targets(Fanout.plan(state, subs, "MESSAGE_CREATE", %{"channel_id" => "21"}, "21", 0)) ==
             [:alice, :owner]
  end

  test "stage events require channel visibility and the GUILDS intent", %{state: state} do
    subs = [sub(:owner, "1"), sub(:hidden, "3"), sub(:no_intent, "2", bot: true, intents: 0)]

    for event <- ~w(STAGE_INSTANCE_CREATE STAGE_INSTANCE_UPDATE STAGE_INSTANCE_DELETE) do
      plan = Fanout.plan(state, subs, event, %{"channel_id" => "11"}, "11", 0)
      assert targets(plan) == [:owner]
    end
  end

  test "guild-wide events reach every subscriber", %{state: state, subs: subs} do
    plan = Fanout.plan(state, subs, "GUILD_UPDATE", %{"id" => "100"}, nil, 0)
    assert targets(plan) == [:alice, :bob, :owner]
  end

  test "intents filter bots", %{state: state} do
    subs = [
      sub(:bot_no_msgs, "3", bot: true, intents: 1),
      sub(:bot_msgs, "3", bot: true, intents: 1 ||| 1 <<< 9)
    ]

    plan = Fanout.plan(state, subs, "MESSAGE_CREATE", %{"channel_id" => "10"}, "10", 0)
    assert targets(plan) == [:bot_msgs]
  end

  test "bots without MESSAGE_CONTENT receive a stripped variant", %{state: state} do
    subs = [
      sub(:plain, "3", bot: true, intents: 1 <<< 9),
      sub(:content, "2", bot: true, intents: 1 <<< 9 ||| 1 <<< 15),
      sub(:user, "1")
    ]

    data = %{"channel_id" => "10", "author" => %{"id" => "1"}, "mentions" => []}
    plan = Fanout.plan(state, subs, "MESSAGE_CREATE", data, "10", 0)
    assert {:plain, :stripped} in plan
    assert {:content, :full} in plan
    assert {:user, :full} in plan
  end

  test "bots without MESSAGE_CONTENT get full content when mentioned or authoring", %{
    state: state
  } do
    subs = [sub(:bot, "3", bot: true, intents: 1 <<< 9)]

    mentioned = %{
      "channel_id" => "10",
      "author" => %{"id" => "1"},
      "mentions" => [%{"id" => "3"}]
    }

    assert Fanout.plan(state, subs, "MESSAGE_CREATE", mentioned, "10", 0) == [{:bot, :full}]

    authored = %{"channel_id" => "10", "author" => %{"id" => "3"}, "mentions" => []}
    assert Fanout.plan(state, subs, "MESSAGE_UPDATE", authored, "10", 0) == [{:bot, :full}]
  end

  test "ban events need BAN_MEMBERS", %{state: state, subs: subs} do
    plan =
      Fanout.plan(
        state,
        subs,
        "GUILD_BAN_ADD",
        %{"guild_id" => "100", "user" => %{"id" => "9"}},
        nil,
        0
      )

    assert targets(plan) == [:alice, :owner]
  end

  test "audit log entries need VIEW_AUDIT_LOG", %{state: state, subs: subs} do
    plan = Fanout.plan(state, subs, "AUDIT_LOG_ENTRY_CREATE", %{"guild_id" => "100"}, nil, 0)
    assert targets(plan) == [:owner]
  end

  test "invite events need MANAGE_CHANNELS on the channel", %{state: state, subs: subs} do
    plan =
      Fanout.plan(state, subs, "INVITE_CREATE", %{"channel_id" => "10", "code" => "x"}, "10", 0)

    assert targets(plan) == [:owner]
  end

  test "command permission updates go to managers and the owning application", %{state: state} do
    subs = [sub(:owner, "1"), sub(:bob, "3"), sub(:app_bot, "3", bot: true, application_id: "77")]
    data = %{"application_id" => "77", "guild_id" => "100"}
    plan = Fanout.plan(state, subs, "APPLICATION_COMMAND_PERMISSIONS_UPDATE", data, nil, 0)
    assert targets(plan) == [:app_bot, :owner]
  end

  test "non-member subscribers never receive guild events", %{state: state} do
    plan = Fanout.plan(state, [sub(:stranger, "999")], "GUILD_UPDATE", %{"id" => "100"}, nil, 0)
    assert plan == []
  end

  test "here_only?/2 only holds for MESSAGE_CREATE with mention_here" do
    assert Fanout.here_only?("MESSAGE_CREATE", %{"mention_here" => true})
    refute Fanout.here_only?("MESSAGE_CREATE", %{"mention_everyone" => true})
    refute Fanout.here_only?("MESSAGE_UPDATE", %{"mention_here" => true})
  end

  test "quiet_here/1 clears the broadcast flags and keeps user mentions" do
    data = %{"mention_everyone" => true, "mention_here" => true, "mentions" => [%{"id" => "2"}]}

    assert Fanout.quiet_here(data) == %{
             "mention_everyone" => false,
             "mentions" => [%{"id" => "2"}]
           }
  end

  test "here_audience?/1 excludes offline and unknown presence" do
    refute Fanout.here_audience?(nil)
    refute Fanout.here_audience?(%{status: "offline"})
    for status <- ~w(online idle dnd), do: assert(Fanout.here_audience?(%{status: status}))
  end

  test "strip_content/1 empties content-bearing fields" do
    data = %{
      "content" => "x",
      "embeds" => [1],
      "attachments" => [1],
      "components" => [1],
      "poll" => %{},
      "id" => "m"
    }

    assert Fanout.strip_content(data) == %{
             "content" => "",
             "embeds" => [],
             "attachments" => [],
             "components" => [],
             "id" => "m"
           }
  end
end
