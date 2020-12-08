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
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"github.com/golang/protobuf/proto"

	pb "cs.utexas.edu/zjia/hipster-shippingservice/genproto"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
)

const (
	defaultPort = "50051"
)

var log *logrus.Logger

func init() {
	log = logrus.New()
	log.Level = logrus.InfoLevel
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	log.Out = os.Stdout
}

func main() {
	dbAddr, ok := os.LookupEnv("SHIPPING_DB_ADDR")
	if !ok {
		log.Fatalf("SHIPPING_DB_ADDR not set")
	}
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://" + dbAddr))
    if err != nil {
        log.Fatalf("mongo.NewClient failed: %v", err)
    }
    ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
    err = client.Connect(ctx)
    if err != nil {
        log.Fatalf("mongo.Connect failed: %v", err)
	}
	
	faas.Serve(&funcHandlerFactory{dbClient: client})
}

// server controls RPC service responses.
type server struct {
	dbClient *mongo.Client
}

// GetQuote produces a shipping quote (cost) in USD.
func (s *server) GetQuote(ctx context.Context, in *pb.GetQuoteRequest) (*pb.GetQuoteResponse, error) {
	log.Info("[GetQuote] received request")
	defer log.Info("[GetQuote] completed request")

	// 1. Our quote system requires the total number of items to be shipped.
	count := 0
	for _, item := range in.Items {
		count += int(item.Quantity)
	}

	// 2. Generate a quote based on the total number of items to be shipped.
	quote := CreateQuoteFromCount(count)

	// 3. Generate a response.
	return &pb.GetQuoteResponse{
		CostUsd: &pb.Money{
			CurrencyCode: "USD",
			Units:        int64(quote.Dollars),
			Nanos:        int32(quote.Cents * 10000000)},
	}, nil

}

// ShipOrder mocks that the requested items will be shipped.
// It supplies a tracking ID for notional lookup of shipment delivery status.
func (s *server) ShipOrder(ctx context.Context, in *pb.ShipOrderRequest) (*pb.ShipOrderResponse, error) {
	log.Info("[ShipOrder] received request")
	defer log.Info("[ShipOrder] completed request")
	// 1. Create a Tracking ID
	baseAddress := fmt.Sprintf("%s, %s, %s", in.Address.StreetAddress, in.Address.City, in.Address.State)
	id := CreateTrackingId(baseAddress)

	collection := s.dbClient.Database("shipping").Collection("shipOrders")
	_, err := collection.InsertOne(ctx, bson.D{{Key: "trackingId", Value: id}})
	if err != nil {
		return nil, err
	}

	// 2. Generate a response.
	return &pb.ShipOrderResponse{
		TrackingId: id,
	}, nil
}

type funcHandlerFactory struct {
	dbClient *mongo.Client
}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (f *funcHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	srv := &server{dbClient: f.dbClient}
	return newGrpcFuncHandler(srv, pb.ShippingMethods)
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
