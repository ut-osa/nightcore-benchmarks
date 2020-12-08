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
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"
	"math/rand"

	pb "cs.utexas.edu/zjia/hipster-productcatalogservice/genproto"

	"github.com/golang/protobuf/jsonpb"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"github.com/golang/protobuf/proto"

	"github.com/go-redis/redis/v7"

	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
)

var (
	cat          []string
	catalogMutex *sync.Mutex
	log          *logrus.Logger
	rdb          *redis.Client

	port = "3550"
)

const productListLength = 16

func init() {
	log = logrus.New()
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
	catalogMutex = &sync.Mutex{}

	rdb = redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_HOST") + ":6379",
		Password: "",
		DB:       0,
	})

	swarmTaskSlot := os.Getenv("SWARM_TASK_SLOT")
	if swarmTaskSlot == "" || swarmTaskSlot == "1" {
		writeCatalogData()
	}
}

func main() {
	flag.Parse()

	faas.Serve(&funcHandlerFactory{})
}

type productCatalog struct{}

func writeCatalogData() {
	catalogJSON, err := ioutil.ReadFile("products.json")
	if err != nil {
		panic(err)
	}
	cat := &pb.ListProductsResponse{}
	if err := jsonpb.Unmarshal(bytes.NewReader(catalogJSON), cat); err != nil {
		panic(err)
	}
	log.Info("successfully parsed product catalog json")
	m := jsonpb.Marshaler{}
	for _, product := range cat.Products {
		productJson, err := m.MarshalToString(product)
		_, err = rdb.Set(product.Id, productJson, 0).Result()
		if err != nil {
			panic(err)
		}
	}
	log.Info("successfully write product catalog to redis")
}

func parseCatalog() []string {
	if len(cat) == 0 {
		catalogMutex.Lock()
		var err error
		cat, err = rdb.Keys("*").Result()
		if err != nil {
			panic(err)
		}
		catalogMutex.Unlock()
	}
	return cat
}

func (p *productCatalog) ListProducts(context.Context, *pb.Empty) (*pb.ListProductsResponse, error) {
	allProducts := parseCatalog()
	subProducts := make([]string, productListLength)
	for i := 0; i < productListLength; i++ {
		subProducts[i] = allProducts[rand.Intn(len(allProducts))]
	}
	jsonResults, err := rdb.MGet(subProducts...).Result()
	if err != nil {
		return nil, err
	}
	results := make([]*pb.Product, productListLength)
	for i := 0; i < productListLength; i++ {
		product := &pb.Product{}
		if err := jsonpb.Unmarshal(strings.NewReader(jsonResults[i].(string)), product); err != nil {
			return nil, err
		}
		results[i] = product
	}
	return &pb.ListProductsResponse{Products: results}, nil
}

func (p *productCatalog) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.Product, error) {
	productJson, err := rdb.Get(req.Id).Result()
	if err == redis.Nil {
		return nil, status.Errorf(codes.NotFound, "no product with ID %s", req.Id)
	} else if err != nil {
		return nil, err
	}
	found := &pb.Product{}
	if err := jsonpb.Unmarshal(strings.NewReader(productJson), found); err != nil {
		return nil, err
	}
	return found, nil
}

func (p *productCatalog) SearchProducts(ctx context.Context, req *pb.SearchProductsRequest) (*pb.SearchProductsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "SearchProducts not implemented")
}

type funcHandlerFactory struct {
}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (f *funcHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	srv := &productCatalog{}
	return newGrpcFuncHandler(srv, pb.ProductCatalogMethods)
}

type grpcFuncHandlerWrapper struct {
	srv         interface{}
	grpcMethods map[string]grpc.MethodDesc
}

func (h *grpcFuncHandlerWrapper) Call(ctx context.Context, method string, requestBytes []byte) ([]byte, error) {
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

func newGrpcFuncHandler(srv interface{}, methods []grpc.MethodDesc) (types.GrpcFuncHandler, error) {
	h := &grpcFuncHandlerWrapper{}
	h.srv = srv
	h.grpcMethods = make(map[string]grpc.MethodDesc)
	for _, methodDesc := range methods {
		h.grpcMethods[methodDesc.MethodName] = methodDesc
	}
	return h, nil
}
