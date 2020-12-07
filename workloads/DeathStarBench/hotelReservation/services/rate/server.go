package rate

import (
	"encoding/json"
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	// "io/ioutil"
	// "log"
	// "os"
	"sort"
	"context"

	pb "github.com/harlow/go-micro-services/services/rate/proto"

	"github.com/bradfitz/gomemcache/memcache"
	"strings"
)

const name = "srv-rate"

// Server implements the rate service
type Server struct {
	MongoSession 	*mgo.Session
	MemcClient *memcache.Client
}

// Run starts the server
func (s *Server) Init() error {
	return nil
}

// GetRates gets rates for hotels for specific date range.
func (s *Server) GetRates(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	res := new(pb.Result)
	// session, err := mgo.Dial("mongodb-rate")
	// if err != nil {
	// 	panic(err)
	// }
	// defer session.Close()

	ratePlans := make(RatePlans, 0)

	session := s.MongoSession.Copy()
	defer session.Close()

	for _, hotelID := range req.HotelIds {
		// first check memcached
		item, err := s.MemcClient.Get(hotelID)
		if err == nil {
			// memcached hit
			rate_strs := strings.Split(string(item.Value), "\n")

			// fmt.Printf("memc hit, hotelId = %s\n", hotelID)
			// fmt.Println(rate_strs)

			for _, rate_str := range rate_strs {
				if len(rate_str) != 0 {
					rate_p := new(pb.RatePlan)
					json.Unmarshal(item.Value, rate_p)
					ratePlans = append(ratePlans, rate_p)
				}
			}
		} else if err == memcache.ErrCacheMiss {

			// fmt.Printf("memc miss, hotelId = %s\n", hotelID)

			// memcached miss, set up mongo connection
			c := session.DB("rate-db").C("inventory")

			memc_str := ""

			tmpRatePlans := make(RatePlans, 0)
			err := c.Find(&bson.M{"hotelId": hotelID}).All(&tmpRatePlans)
			if err != nil {
				panic(err)
			} else {
				for _, r := range tmpRatePlans {
					ratePlans = append(ratePlans, r)
					rate_json , err := json.Marshal(r)
					if err != nil {
						fmt.Printf("json.Marshal err = %s\n", err)
					}
					memc_str = memc_str + string(rate_json) + "\n"
				}
			}

			// write to memcached
			s.MemcClient.Set(&memcache.Item{Key: hotelID, Value: []byte(memc_str)})

		} else {
			fmt.Printf("Memmcached error = %s\n", err)
			panic(err)
		}
	}

	sort.Sort(ratePlans)
	res.RatePlans = ratePlans

	return res, nil
}

type RatePlans []*pb.RatePlan

func (r RatePlans) Len() int {
	return len(r)
}

func (r RatePlans) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r RatePlans) Less(i, j int) bool {
	return r[i].RoomType.TotalRate > r[j].RoomType.TotalRate
}
