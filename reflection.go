package grpc

import (
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/reflect/protodesc"
)

type Reflector interface {
	protodesc.Resolver
	reflection.ServiceInfoProvider
	reflection.ExtensionResolver
}

const (
	// ReflectV1ServiceName is the fully-qualified name of the v1 version of the reflection service.
	ReflectV1ServiceName = "grpc.reflection.v1.ServerReflection"
	// ReflectServiceURLPathV1 is the full path for reflection service endpoint
	ReflectServiceURLPathV1 = "/" + ReflectV1ServiceName + "/"
	// ReflectMethodName is the reflection service name
	ReflectMethodName = "ServerReflectionInfo"
)
