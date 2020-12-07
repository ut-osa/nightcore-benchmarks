package recommendation

import (
	// "encoding/json"
	"github.com/hailocab/go-geoindex"
	pb "github.com/harlow/go-micro-services/services/recommendation/proto"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	// "io/ioutil"
	"log"
	"math"
	// "os"
	"context"

	// "strings"
)

const name = "srv-recommendation"
const maxRecommendationResults = 5

// Server implements the recommendation service
type Server struct {
	hotels map[string]Hotel
	MongoSession	*mgo.Session
}

// Run starts the server
func (s *Server) Init() error {
	return nil
}

// GiveRecommendation returns recommendations within a given requirement.
func (s *Server) GetRecommendations(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	if s.hotels == nil {
		s.hotels = loadRecommendations(s.MongoSession)
	}

	res := new(pb.Result)
	// fmt.Printf("GetRecommendations\n")
	require := req.Require
	if require == "dis" {
		p1 := &geoindex.GeoPoint{
			Pid:  "",
			Plat: req.Lat,
			Plon: req.Lon,
		}
		min := math.MaxFloat64
		for _, hotel := range s.hotels {
			tmp := float64(geoindex.Distance(p1, &geoindex.GeoPoint{
				Pid:  "",
				Plat: hotel.HLat,
				Plon: hotel.HLon,
			})) / 1000
			if tmp < min {
				min = tmp
			}
		}
		for _, hotel := range s.hotels {
			tmp := float64(geoindex.Distance(p1, &geoindex.GeoPoint{
				Pid:  "",
				Plat: hotel.HLat,
				Plon: hotel.HLon,
			})) / 1000
			if tmp == min && len(res.HotelIds) < maxRecommendationResults {
				res.HotelIds = append(res.HotelIds, hotel.HId)
			}
		}
	} else if require == "rate" {
		max := 0.0
		for _, hotel := range s.hotels {
			if hotel.HRate > max {
				max = hotel.HRate
			}
		}
		for _, hotel := range s.hotels {
			if hotel.HRate == max && len(res.HotelIds) < maxRecommendationResults {
				res.HotelIds = append(res.HotelIds, hotel.HId)
			}
		}
	} else if require == "price" {
		min := math.MaxFloat64
		for _, hotel := range s.hotels {
			if hotel.HPrice < min {
				min = hotel.HPrice
			}
		}
		for _, hotel := range s.hotels {
			if hotel.HPrice == min && len(res.HotelIds) < maxRecommendationResults {
				res.HotelIds = append(res.HotelIds, hotel.HId)
			}
		}
	} else {
		log.Println("Wrong parameter: ", require)
	}

	return res, nil
}

// loadRecommendations loads hotel recommendations from mongodb.
func loadRecommendations(session *mgo.Session) map[string]Hotel {
	// session, err := mgo.Dial("mongodb-recommendation")
	// if err != nil {
	// 	panic(err)
	// }
	// defer session.Close()
	s := session.Copy()
	defer s.Close()

	c := s.DB("recommendation-db").C("recommendation")

	// unmarshal json profiles
	var hotels []Hotel
	err := c.Find(bson.M{}).All(&hotels)
	if err != nil {
		log.Println("Failed get hotels data: ", err)
	}

	profiles := make(map[string]Hotel)
	for _, hotel := range hotels {
		profiles[hotel.HId] = hotel
	}

	return profiles
}

type Hotel struct {
	ID     bson.ObjectId `bson:"_id"`
	HId    string        `bson:"hotelId"`
	HLat   float64       `bson:"lat"`
	HLon   float64       `bson:"lon"`
	HRate  float64       `bson:"rate"`
	HPrice float64       `bson:"price"`
}