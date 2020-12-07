package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
	"gopkg.in/mgo.v2"
	"github.com/bradfitz/gomemcache/memcache"

	"github.com/harlow/go-micro-services/services/profile"
	"github.com/harlow/go-micro-services/utils"
	pb "github.com/harlow/go-micro-services/services/profile/proto"
	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
)

type funcHandlerFactory struct {
	mongoSession *mgo.Session
	memcClient   *memcache.Client
}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (f *funcHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	if service != "profile.Profile" {
		return nil, fmt.Errorf("Unknown service: %s", service)
	}
	srv := &profile.Server{MongoSession: f.mongoSession, MemcClient: f.memcClient}
	err := srv.Init()
	if err != nil {
		return nil, err
	}
	return utils.NewGrpcFuncHandler(srv, pb.ProfileMethods)
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

	mongo_session := initializeDatabase(result["ProfileMongoAddress"])
	defer mongo_session.Close()

	fmt.Printf("profile memc addr port = %s\n", result["ProfileMemcAddress"])
	memc_client := memcache.New(result["ProfileMemcAddress"])
	memc_client.Timeout = 100 * time.Millisecond
	memc_client.MaxIdleConns = 64

	faas.Serve(&funcHandlerFactory{mongoSession: mongo_session, memcClient: memc_client})
}
