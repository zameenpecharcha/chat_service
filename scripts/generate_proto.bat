@echo off
setlocal
cd /d %~dp0\..
if not exist app\pb mkdir app\pb
protoc ^^
  --proto_path=proto ^^
  --go_out=app/pb --go_opt=paths=source_relative ^^
  --go-grpc_out=app/pb --go-grpc_opt=paths=source_relative ^^
  proto/chat.proto
if %ERRORLEVEL% neq 0 (
  echo protoc failed. Ensure protoc and protoc-gen-go / protoc-gen-go-grpc are on PATH.
  exit /b %ERRORLEVEL%
)
echo Done.


