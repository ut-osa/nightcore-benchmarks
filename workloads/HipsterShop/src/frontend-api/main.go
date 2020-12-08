// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"os"
	"time"
	"strings"
	"encoding/json"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"github.com/golang/protobuf/proto"

	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
)

const (
	port            = "8080"
	defaultCurrency = "USD"
	cookieMaxAge    = 60 * 60 * 48

	cookiePrefix    = "shop_"
	cookieSessionID = cookiePrefix + "session-id"
	cookieCurrency  = cookiePrefix + "currency"
)

var (
	whitelistedCurrencies = map[string]bool{
		"USD": true,
		"EUR": true,
		"CAD": true,
		"JPY": true,
		"GBP": true,
		"TRY": true}
)

type ctxKeySessionID struct{}
type ctxKeyLog struct{}

type frontendServer struct {
	funcName string
	conn     grpc.ClientConnInterface
	log      *logrus.Logger
}

func (fe *frontendServer) Call(ctx context.Context, input []byte) ([]byte, error) {
	jsonInput := make(map[string]string)
	err := json.Unmarshal(input, &jsonInput)
	if err != nil {
		return nil, fmt.Errorf("JSON unmarshal failed: %v", err)
	}
	log := fe.log.WithFields(logrus.Fields{})
	ctx = context.WithValue(ctx, ctxKeyLog{}, log)
	var output map[string]interface{}
	if fe.funcName == "home" {
		output, err = fe.homeHandler(ctx, jsonInput)
	} else if fe.funcName == "product" {
		output, err = fe.productHandler(ctx, jsonInput)
	} else if fe.funcName == "viewCart" {
		output, err = fe.viewCartHandler(ctx, jsonInput)
	} else if fe.funcName == "addToCart" {
		output, err = fe.addToCartHandler(ctx, jsonInput)
	} else if fe.funcName == "checkout" {
		output, err = fe.placeOrderHandler(ctx, jsonInput)
	} else {
		panic(fmt.Errorf("Unknown func name: %s", fe.funcName))
	}
	if err != nil {
		return nil, err
	} else {
		return json.Marshal(output)
	}
}

func main() {
	log := logrus.New()
	if os.Getenv("DISABLE_LOGGING") == "1" {
		log.Level = logrus.FatalLevel
	} else {
		log.Level = logrus.InfoLevel
	}
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	log.Out = os.Stdout

	faas.Serve(&funcHandlerFactory{log: log})
}

type funcHandlerFactory struct {
	log *logrus.Logger
}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	conn, err := newGrpcClientConn(env)
	if err != nil {
		return nil, err
	}
	srv := &frontendServer{funcName: funcName, conn: conn, log: f.log}
	return srv, nil
}

func (f *funcHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	return nil, fmt.Errorf("Not implemented")
}

type grpcClientConnWrapper struct {
	env types.Environment
}

func (c *grpcClientConnWrapper) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
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

func (h *grpcClientConnWrapper) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Errorf(codes.Unimplemented, "NewStream not implemented")
}

func newGrpcClientConn(env types.Environment) (grpc.ClientConnInterface, error) {
	return &grpcClientConnWrapper{env: env}, nil
}
