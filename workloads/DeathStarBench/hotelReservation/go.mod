module github.com/harlow/go-micro-services

go 1.14

require (
	cs.utexas.edu/zjia/faas v0.0.0
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/golang/protobuf v1.4.0
	github.com/hailocab/go-geoindex v0.0.0-20160127134810-64631bfe9711
	google.golang.org/grpc v1.28.0
	google.golang.org/protobuf v1.23.0
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22
)

replace cs.utexas.edu/zjia/faas => /src/nightcore/worker/golang
