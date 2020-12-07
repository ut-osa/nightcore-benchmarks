package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"gopkg.in/mgo.v2"

	"github.com/harlow/go-micro-services/services/rate"
	"github.com/harlow/go-micro-services/utils"
	pb "github.com/harlow/go-micro-services/services/rate/proto"
	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"

	"github.com/bradfitz/gomemcache/memcache"
	"time"
)

type funcHandlerFactory struct {
	mongoSession *mgo.Session
	memcClient   *memcache.Client
}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (f *funcHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	if service != "rate.Rate" {
		return nil, fmt.Errorf("Unknown service: %s", service)
	}
	srv := &rate.Server{MongoSession: f.mongoSession, MemcClient: f.memcClient}
	err := srv.Init()
	if err != nil {
		return nil, err
	}
	return utils.NewGrpcFuncHandler(srv, pb.RateMethods)
}

func main() {
	jsonFile, err := os.Open("config.json")
	if err != nil {
		fmt.Println(err)
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var result map[string]string
	json.Unmarshal([]byte(byteValue), &result)

	mongo_session := initializeDatabase(result["RateMongoAddress"])

	fmt.Printf("rate memc addr port = %s\n", result["RateMemcAddress"])
	memc_client := memcache.New(result["RateMemcAddress"])
	memc_client.Timeout = 100 * time.Millisecond
	memc_client.MaxIdleConns = 64

	defer mongo_session.Close()
	
	faas.Serve(&funcHandlerFactory{mongoSession: mongo_session, memcClient: memc_client})
}
