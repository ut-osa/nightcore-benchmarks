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
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
	"math/rand"
	"encoding/json"

	pb "cs.utexas.edu/zjia/hipster-adservice/genproto"

	"github.com/golang/protobuf/jsonpb"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"github.com/golang/protobuf/proto"

	"github.com/go-redis/redis/v7"

	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
)

var (
	log  *logrus.Logger
	rdb  *redis.Client

	port = "3550"
)

const randomAdsToServe = 2

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

	rdb = redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_HOST") + ":6379",
		Password: "",
		DB:       0,
	})

	swarmTaskSlot := os.Getenv("SWARM_TASK_SLOT")
	if swarmTaskSlot == "" || swarmTaskSlot == "1" {
		writeAdData()
	}
}

func main() {
	flag.Parse()
	faas.Serve(&funcHandlerFactory{})
}

type funcHandlerFactory struct {}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (f *funcHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	srv := &adServer{}
	return newGrpcFuncHandler(srv, pb.AdMethods)
}

type adServer struct{}

func writeAdData() {
	adsJSON, err := ioutil.ReadFile("ads.json")
	if err != nil {
		panic(err)
	}
	ads := make(map[string]([]*pb.Ad))
	if err := json.Unmarshal(adsJSON, &ads); err != nil {
		panic(err)
	}
	log.Info("successfully parsed product ads json")
	m := jsonpb.Marshaler{}
	for category, subAds := range ads {
		for _, ad := range subAds {
			adJson, err := m.MarshalToString(ad)
			if err != nil {
				panic(err)
			}
			_, err = rdb.LPush(category, adJson).Result()
			if err != nil {
				panic(err)
			}
		}
	}
	log.Info("successfully write product ads to redis")
}

func (p *adServer) GetAds(ctx context.Context, req *pb.AdRequest) (*pb.AdResponse, error) {
	results := make([]*pb.Ad, 0)
	for _, category := range req.ContextKeys {
		subAds, err := rdb.LRange(category, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		for _, adJson := range subAds {
			ad := &pb.Ad{}
			if err := jsonpb.Unmarshal(strings.NewReader(adJson), ad); err != nil {
				return nil, err
			}
			results = append(results, ad)
		}
	}
	if len(results) == 0 {
		randomAds, err := getRandomAds();
		if err != nil {
			return nil, err
		}
		return &pb.AdResponse{Ads: randomAds}, nil
	} else {
		return &pb.AdResponse{Ads: results}, nil
	}
}

func getRandomAds() ([]*pb.Ad, error) {
	results := make([]*pb.Ad, randomAdsToServe)
	for i := 0; i < randomAdsToServe; i++ {
		var subAds []string
		for {
			category, err := rdb.RandomKey().Result()
			if err != nil {
				return nil, err
			}
			subAds, err = rdb.LRange(category, 0, -1).Result()
			if err != nil {
				return nil, err
			}
			if len(subAds) > 0 {
				break
			}
		}
		ad := &pb.Ad{}
		adJson := subAds[rand.Intn(len(subAds))]
		if err := jsonpb.Unmarshal(strings.NewReader(adJson), ad); err != nil {
			return nil, err
		}
		results[i] = ad
	}
	return results, nil
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
