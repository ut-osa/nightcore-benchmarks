package user

import (
	"crypto/sha256"
	// "encoding/json"
	"fmt"
	"context"
	pb "github.com/harlow/go-micro-services/services/user/proto"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	// "io/ioutil"
	"log"
)

const name = "srv-user"

// Server implements the user service
type Server struct {
	users map[string]string

	MongoSession 	*mgo.Session
}

// Run starts the server
func (s *Server) Init() error {
	return nil
}

// CheckUser returns whether the username and password are correct.
func (s *Server) CheckUser(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	if s.users == nil {
		s.users = loadUsers(s.MongoSession)
	}

	res := new(pb.Result)

	// fmt.Printf("CheckUser")

	sum := sha256.Sum256([]byte(req.Password))
	pass := fmt.Sprintf("%x", sum)

	// session, err := mgo.Dial("mongodb-user")
	// if err != nil {
	// 	panic(err)
	// }
	// defer session.Close()

	// c := session.DB("user-db").C("user")

	// user := User{}
	// err = c.Find(bson.M{"username": req.Username}).One(&user)
	// if err != nil {
	// 	panic(err)
	// }
	res.Correct = false
	if true_pass, found := s.users[req.Username]; found {
	    res.Correct = pass == true_pass
	}
	
	// res.Correct = user.Password == pass

	// fmt.Printf("CheckUser %d\n", res.Correct)

	return res, nil
}

// loadUsers loads hotel users from mongodb.
func loadUsers(session *mgo.Session) map[string]string {
	// session, err := mgo.Dial("mongodb-user")
	// if err != nil {
	// 	panic(err)
	// }
	// defer session.Close()
	s := session.Copy()
	defer s.Close()
	c := s.DB("user-db").C("user")

	// unmarshal json profiles
	var users []User
	err := c.Find(bson.M{}).All(&users)
	if err != nil {
		log.Println("Failed get users data: ", err)
	}

	res := make(map[string]string)
	for _, user := range users {
		res[user.Username] = user.Password
	}

	fmt.Printf("Done load users\n")

	return res
}

type User struct {
	Username string `bson:"username"`
	Password string `bson:"password"`
}