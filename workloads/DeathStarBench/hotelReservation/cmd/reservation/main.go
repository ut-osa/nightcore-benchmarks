package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"gopkg.in/mgo.v2"

	pb "github.com/harlow/go-micro-services/services/reservation/proto"
	"github.com/harlow/go-micro-services/services/reservation"
	"github.com/harlow/go-micro-services/utils"
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
	if service != "reservation.Reservation" {
		return nil, fmt.Errorf("Unknown service: %s", service)
	}
	srv := &reservation.Server{MongoSession: f.mongoSession, MemcClient: f.memcClient}
	err := srv.Init()
	if err != nil {
		return nil, err
	}
	return utils.NewGrpcFuncHandler(srv, pb.ReservationMethods)
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

	mongo_session := initializeDatabase(result["ReserveMongoAddress"])
	defer mongo_session.Close()

	fmt.Printf("reservation memc addr port = %s\n", result["ReserveMemcAddress"])
	memc_client := memcache.New(result["ReserveMemcAddress"])
	memc_client.Timeout = 100 * time.Millisecond
	memc_client.MaxIdleConns = 64

	faas.Serve(&funcHandlerFactory{mongoSession: mongo_session, memcClient: memc_client})
}