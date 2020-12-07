package main

import (
	"fmt"

	"github.com/harlow/go-micro-services/services/frontend"
	"github.com/harlow/go-micro-services/utils"
	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
)

type funcHandlerFactory struct {
}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	conn, err := utils.NewGrpcClientConn(env)
	if err != nil {
		return nil, err
	}
	srv := &frontend.Server{FuncName: funcName}
	err = srv.Init(conn)
	if err != nil {
		return nil, err
	}
	return srv, nil
}

func (f *funcHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	return nil, fmt.Errorf("Not implemented")
}

func main() {
	faas.Serve(&funcHandlerFactory{})
}
