alias DiscordyGateway.Interface.Socket

{:ok, context} = :zstd.context(:compress)
state = %{session: self(), compression: context}
frames = [
  {"READY", %{"op" => 0, "t" => "READY", "d" => %{"session_id" => "abc", "user" => %{"id" => "5"}}}},
  {"MESSAGE_CREATE", %{"op" => 0, "t" => "MESSAGE_CREATE", "d" => %{"content" => "hello", "id" => "123"}}},
  {"RESUMED", %{"op" => 0, "t" => "RESUMED", "d" => %{}}}
]
IO.puts("Event | plain bytes | compressed bytes (same stream)")
for {name, frame} <- frames do
  json = Jason.encode!(frame)
  {:push, {:binary, bytes}, ^state} = Socket.handle_info({:gw_frame, json}, state)
  IO.puts("#{name} | #{byte_size(json)} | #{byte_size(bytes)}")
end
:zstd.close(context)
