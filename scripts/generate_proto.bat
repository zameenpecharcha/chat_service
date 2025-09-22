@echo off
setlocal
cd /d %~dp0\..
protoc --go_out=internal/pb --go_opt=paths=source_relative --go-grpc_out=internal/pb --go-grpc_opt=paths=source_relative proto/chat.proto
echo Done.


