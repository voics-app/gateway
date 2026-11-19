alias DiscordyGateway.Interface.Socket

{:ok, context} = :zstd.context(:compress)
state = %{session: self(), compression: context}
frames = [
  {"READY", %{"op" => 0, "t" => "READY", "s" => 1, "d" => %{"session_id" => "session-123", "user" => %{"id" => "123", "username" => "example"}, "guilds" => []}}},
  {"MESSAGE_CREATE", %{"op" => 0, "t" => "MESSAGE_CREATE", "s" => 2, "d" => %{"id" => "987", "channel_id" => "123", "author" => %{"id" => "123", "username" => "example"}, "content" => "Hello from a gateway session"}}},
  {"RESUMED", %{"op" => 0, "t" => "RESUMED", "s" => 3, "d" => %{}}}
]
IO.puts("Event | plain bytes | compressed bytes (same stream)")
for {name, frame} <- frames do
  json = Jason.encode!(frame)
  {:push, {:binary, bytes}, ^state} = Socket.handle_info({:gw_frame, json}, state)
  IO.puts("#{name} | #{byte_size(json)} | #{byte_size(bytes)}")
end
:zstd.close(context)
