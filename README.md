Tcplayer is a tool to capture realtime/offline network traffic, reassemble tcp packets, analyze application layer protocol(like http/thrift/grpc) requests, magnify and replay to remote servers.

`raw network traffic -> tcp reassembly -> http/thrift/grpc application requests(maybe clone and magnify) -> remote servers`

features:
+ online/offline/tcp traffic capture mode
+ both long and short connection traffic support(especially long connection, this is difficult for other IP layer replay tools like tcpcopy)
+ traffic clone and magnify support(request level and connection level)
+ concurrent clients support
+ long and short connection for remote servers support
+ easy to add new application layer protocol

usage:
`go run cmd/tcplayer.go -h`

