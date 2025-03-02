package grpc

//go:generate go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
//go:generate sh -c "protoc -I./proto -I$(go list -f '{{ .Dir }}' -m go.unistack.org/micro-proto/v4) -I. --go-grpc_out=paths=source_relative:./proto --go_out=paths=source_relative:./proto proto/test.proto"
