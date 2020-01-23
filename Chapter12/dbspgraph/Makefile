include ../../Makefile

proto: ensure-proto-deps
	@echo "[protoc] generating protos for API"
	@protoc --go_out=plugins=grpc:proto/ -Iproto/ api.proto

ensure-proto-deps:
	@echo "[go get] ensuring protoc packages are available"
	@go get google.golang.org/grpc
	@go get github.com/golang/protobuf/protoc-gen-go
	
