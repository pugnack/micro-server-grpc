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
	// ReflectV1AlphaServiceName is the fully-qualified name of the v1alpha version of the reflection service.
	ReflectV1AlphaServiceName = "grpc.reflection.v1alpha.ServerReflection"

	ReflectServiceURLPathV1      = "/" + ReflectV1ServiceName + "/"
	ReflectServiceURLPathV1Alpha = "/" + ReflectV1AlphaServiceName + "/"
	ReflectMethodName            = "ServerReflectionInfo"
)
