module cs.utexas.edu/zjia/hipster-shippingservice

go 1.14

require (
	cs.utexas.edu/zjia/faas v0.0.0
	github.com/golang/protobuf v1.3.1
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.6.2
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.4.2
	go.mongodb.org/mongo-driver v1.3.5
	golang.org/x/net v0.0.0-20190404232315-eb5bcb51f2a3
	google.golang.org/grpc v1.22.0
)

replace cs.utexas.edu/zjia/faas => /src/nightcore/worker/golang
