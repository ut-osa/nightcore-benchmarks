package main

import (
	"fmt"

	pb "github.com/harlow/go-micro-services/services/search/proto"
	"github.com/harlow/go-micro-services/services/search"
	"github.com/harlow/go-micro-services/utils"
	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
)

type funcHandlerFactory struct {
}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (f *funcHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	if service != "search.Search" {
		return nil, fmt.Errorf("Unknown service: %s", service)
	}
	conn, err := utils.NewGrpcClientConn(env)
	if err != nil {
		return nil, err
	}
	srv := &search.Server{}
	err = srv.Init(conn)
	if err != nil {
		return nil, err
	}
	return utils.NewGrpcFuncHandler(srv, pb.SearchMethods)
}

func main() {
	faas.Serve(&funcHandlerFactory{})
}
