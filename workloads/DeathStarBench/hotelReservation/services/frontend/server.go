package frontend

import (
	"encoding/json"
	"fmt"
	"github.com/harlow/go-micro-services/services/recommendation/proto"
	"github.com/harlow/go-micro-services/services/reservation/proto"
	"github.com/harlow/go-micro-services/services/user/proto"
	"strconv"
	"context"
	"google.golang.org/grpc"

	"github.com/harlow/go-micro-services/services/profile/proto"
	"github.com/harlow/go-micro-services/services/search/proto"
)

// Server implements frontend service
type Server struct {
	FuncName             string
	searchClient         search.SearchClient
	profileClient        profile.ProfileClient
	recommendationClient recommendation.RecommendationClient
	userClient           user.UserClient
	reservationClient    reservation.ReservationClient
}

func (s *Server) Init(conn grpc.ClientConnInterface) error {
	s.searchClient = search.NewSearchClient(conn)
	s.profileClient = profile.NewProfileClient(conn)
	s.recommendationClient = recommendation.NewRecommendationClient(conn)
	s.userClient = user.NewUserClient(conn)
	s.reservationClient = reservation.NewReservationClient(conn)
	return nil
}

func (s *Server) Call(ctx context.Context, input []byte) ([]byte, error) {
	jsonInput := make(map[string]string)
	err := json.Unmarshal(input, &jsonInput)
	if err != nil {
		return nil, fmt.Errorf("JSON unmarshal failed: %v", err)
	}
	var output map[string]interface{}
	if s.FuncName == "hotels" {
		output, err = s.searchHandler(ctx, jsonInput)
	} else if s.FuncName == "recommendations" {
		output, err = s.recommendHandler(ctx, jsonInput)
	} else if s.FuncName == "user" {
		output, err = s.userHandler(ctx, jsonInput)
	} else if s.FuncName == "reservation" {
		output, err = s.reservationHandler(ctx, jsonInput)
	} else {
		return nil, fmt.Errorf("Unknown func name: %s", s.FuncName)
	}
	if err != nil {
		return nil, err
	} else {
		return json.Marshal(output)
	}
}

func (s *Server) searchHandler(ctx context.Context, input map[string]string) (map[string]interface{}, error) {
	// fmt.Printf("starts searchHandler\n")

	// in/out dates from query params
	inDate, exist := input["inDate"]
	if !exist {
		return nil, fmt.Errorf("Please specify inDate params")
	}
	outDate, exist := input["outDate"]
	if !exist {
		return nil, fmt.Errorf("Please specify outDate params")
	}

	// lan/lon from query params
	sLat, exist := input["lat"]
	if !exist {
		return nil, fmt.Errorf("Please specify location params")
	}
	sLon, exist := input["lon"]
	if !exist {
		return nil, fmt.Errorf("Please specify location params")
	}

	Lat, _ := strconv.ParseFloat(sLat, 32)
	lat := float32(Lat)
	Lon, _ := strconv.ParseFloat(sLon, 32)
	lon := float32(Lon)

	// fmt.Printf("starts searchHandler querying downstream\n")

	// search for best hotels
	searchResp, err := s.searchClient.Nearby(ctx, &search.NearbyRequest{
		Lat:     lat,
		Lon:     lon,
		InDate:  inDate,
		OutDate: outDate,
	})
	if err != nil {
		return nil, err
	}

	// fmt.Printf("searchHandler gets searchResp\n")
	// for _, hid := range searchResp.HotelIds {
	// 	fmt.Printf("search Handler hotelId = %s\n", hid)
	// }

	// grab locale from query params or default to en
	locale, exist := input["locale"]
	if !exist {
		locale = "en"
	}

	reservationResp, err := s.reservationClient.CheckAvailability(ctx, &reservation.Request{
		CustomerName: "",
		HotelId:      searchResp.HotelIds,
		InDate:       inDate,
		OutDate:      outDate,
		RoomNumber:   1,
	})

	// fmt.Printf("searchHandler gets reserveResp\n")
	// fmt.Printf("searchHandler gets reserveResp.HotelId = %s\n", reservationResp.HotelId)

	// hotel profiles
	profileResp, err := s.profileClient.GetProfiles(ctx, &profile.Request{
		HotelIds: reservationResp.HotelId,
		Locale:   locale,
	})
	if err != nil {
		return nil, err
	}

	return geoJSONResponse(profileResp.Hotels), nil
}

func (s *Server) recommendHandler(ctx context.Context, input map[string]string) (map[string]interface{}, error) {
	sLat, exist := input["lat"]
	if !exist {
		return nil, fmt.Errorf("Please specify location params")
	}
	sLon, exist := input["lon"]
	if !exist {
		return nil, fmt.Errorf("Please specify location params")
	}
	Lat, _ := strconv.ParseFloat(sLat, 64)
	lat := float64(Lat)
	Lon, _ := strconv.ParseFloat(sLon, 64)
	lon := float64(Lon)

	require, exist := input["require"]
	if !exist {
		return nil, fmt.Errorf("Please specify require params")
	}
	if require != "dis" && require != "rate" && require != "price" {
		return nil, fmt.Errorf("Please specify require params")
	}

	// recommend hotels
	recResp, err := s.recommendationClient.GetRecommendations(ctx, &recommendation.Request{
		Require: require,
		Lat:     float64(lat),
		Lon:     float64(lon),
	})
	if err != nil {
		return nil, err
	}

	// grab locale from query params or default to en
	locale, exist := input["locale"]
	if !exist {
		locale = "en"
	}

	// hotel profiles
	profileResp, err := s.profileClient.GetProfiles(ctx, &profile.Request{
		HotelIds: recResp.HotelIds,
		Locale:   locale,
	})
	if err != nil {
		return nil, err
	}

	return geoJSONResponse(profileResp.Hotels), nil
}

func (s *Server) userHandler(ctx context.Context, input map[string]string) (map[string]interface{}, error) {
	username, exist := input["username"]
	if !exist {
		return nil, fmt.Errorf("Please specify username params")
	}
	password, exist := input["password"]
	if !exist {
		return nil, fmt.Errorf("Please specify password params")
	}

	// Check username and password
	recResp, err := s.userClient.CheckUser(ctx, &user.Request{
		Username: username,
		Password: password,
	})
	if err != nil {
		return nil, err
	}

	str := "Login successfully!"
	if recResp.Correct == false {
		str = "Failed. Please check your username and password. "
	}

	res := map[string]interface{}{
		"message": str,
	}

	return res, nil
}

func (s *Server) reservationHandler(ctx context.Context, input map[string]string) (map[string]interface{}, error) {
	inDate, exist := input["inDate"]
	if !exist {
		return nil, fmt.Errorf("Please specify inDate params")
	}
	outDate, exist := input["outDate"]
	if !exist {
		return nil, fmt.Errorf("Please specify outDate params")
	}

	if !checkDataFormat(inDate) || !checkDataFormat(outDate) {
		return nil, fmt.Errorf("Please check inDate/outDate format (YYYY-MM-DD)")
	}

	hotelId, exist := input["hotelId"]
	if !exist {
		return nil, fmt.Errorf("Please specify hotelId params")
	}

	customerName, exist := input["customerName"]
	if !exist {
		return nil, fmt.Errorf("Please specify customerName params")
	}

	username, exist := input["username"]
	if !exist {
		return nil, fmt.Errorf("Please specify username params")
	}
	password, exist := input["password"]
	if !exist {
		return nil, fmt.Errorf("Please specify password params")
	}

	numberOfRoom := 0
	num, exist := input["number"]
	if exist {
		numberOfRoom, _ = strconv.Atoi(num)
	}

	// Check username and password
	recResp, err := s.userClient.CheckUser(ctx, &user.Request{
		Username: username,
		Password: password,
	})
	if err != nil {
		return nil, err
	}

	str := "Reserve successfully!"
	if recResp.Correct == false {
		str = "Failed. Please check your username and password. "
	}

	// Make reservation
	resResp, err := s.reservationClient.MakeReservation(ctx, &reservation.Request{
		CustomerName: customerName,
		HotelId:      []string{hotelId},
		InDate:       inDate,
		OutDate:      outDate,
		RoomNumber:   int32(numberOfRoom),
	})
	if err != nil {
		return nil, err
	}
	if len(resResp.HotelId) == 0 {
		str = "Failed. Already reserved. "
	}

	res := map[string]interface{}{
		"message": str,
	}

	return res, nil
}

// return a geoJSON response that allows google map to plot points directly on map
// https://developers.google.com/maps/documentation/javascript/datalayer#sample_geojson
func geoJSONResponse(hs []*profile.Hotel) map[string]interface{} {
	fs := []interface{}{}

	for _, h := range hs {
		fs = append(fs, map[string]interface{}{
			"type": "Feature",
			"id":   h.Id,
			"properties": map[string]string{
				"name":         h.Name,
				"phone_number": h.PhoneNumber,
			},
			"geometry": map[string]interface{}{
				"type": "Point",
				"coordinates": []float32{
					h.Address.Lon,
					h.Address.Lat,
				},
			},
		})
	}

	return map[string]interface{}{
		"type":     "FeatureCollection",
		"features": fs,
	}
}

func checkDataFormat(date string) bool {
	if len(date) != 10 {
		return false
	}
	for i := 0; i < 10; i++ {
		if i == 4 || i == 7 {
			if date[i] != '-' {
				return false
			}
		} else {
			if date[i] < '0' || date[i] > '9' {
				return false
			}
		}
	}
	return true
}