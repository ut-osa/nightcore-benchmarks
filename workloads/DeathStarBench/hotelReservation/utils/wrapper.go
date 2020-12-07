package utils

import (
	"context"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"cs.utexas.edu/zjia/faas/types"
)

type GrpcFuncHandlerWrapper struct {
	srv         interface{}
	grpcMethods map[string]grpc.MethodDesc
}

func (h *GrpcFuncHandlerWrapper) Call(ctx context.Context, method string, requestBytes []byte) ([]byte, error) {
	desc, exist := h.grpcMethods[method]
	if !exist {
		return nil, fmt.Errorf("Cannot handle method %s", method)
	}
	reply, err := desc.Handler(
		h.srv, ctx,
		func(request interface{}) error {
			return proto.Unmarshal(requestBytes, request.(proto.Message))
		}, nil)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(reply.(proto.Message))
}

func NewGrpcFuncHandler(srv interface{}, methods []grpc.MethodDesc) (types.GrpcFuncHandler, error) {
	h := &GrpcFuncHandlerWrapper{}
	h.srv = srv
	h.grpcMethods = make(map[string]grpc.MethodDesc)
	for _, methodDesc := range methods {
		h.grpcMethods[methodDesc.MethodName] = methodDesc
	}
	return h, nil
}

type GrpcClientConnWrapper struct {
	env types.Environment
}

func (c *GrpcClientConnWrapper) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	parts := strings.Split(method, "/")
	serviceName := parts[1]
	methodName := parts[2]
	requestBytes, err := proto.Marshal(args.(proto.Message))
	if err != nil {
		return err
	}
	replyBytes, err := c.env.GrpcCall(ctx, serviceName, methodName, requestBytes)
	if err != nil {
		return err
	}
	return proto.Unmarshal(replyBytes, reply.(proto.Message))
}

func (h *GrpcClientConnWrapper) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Errorf(codes.Unimplemented, "NewStream not implemented")
}

func NewGrpcClientConn(env types.Environment) (grpc.ClientConnInterface, error) {
	return &GrpcClientConnWrapper{env: env}, nil
}
