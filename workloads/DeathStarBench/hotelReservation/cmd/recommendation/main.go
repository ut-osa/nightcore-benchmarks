package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"gopkg.in/mgo.v2"

	pb "github.com/harlow/go-micro-services/services/recommendation/proto"
	"github.com/harlow/go-micro-services/services/recommendation"
	"github.com/harlow/go-micro-services/utils"
	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"

	// "github.com/bradfitz/gomemcache/memcache"
)

type funcHandlerFactory struct {
	mongoSession *mgo.Session
}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (f *funcHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	if service != "recommendation.Recommendation" {
		return nil, fmt.Errorf("Unknown service: %s", service)
	}
	srv := &recommendation.Server{MongoSession: f.mongoSession}
	err := srv.Init()
	if err != nil {
		return nil, err
	}
	return utils.NewGrpcFuncHandler(srv, pb.RecommendationMethods)
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

	mongo_session := initializeDatabase(result["RecommendMongoAddress"])
	defer mongo_session.Close()

	faas.Serve(&funcHandlerFactory{mongoSession: mongo_session})
}
