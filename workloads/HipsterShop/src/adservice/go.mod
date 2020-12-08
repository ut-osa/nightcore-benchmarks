module cs.utexas.edu/zjia/hipster-adservice

go 1.14

require (
	cs.utexas.edu/zjia/faas v0.0.0
	github.com/go-redis/redis/v7 v7.4.0
	github.com/golang/protobuf v1.3.2
	github.com/google/go-cmp v0.2.0
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.6.2
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.0.6
	golang.org/x/net v0.0.0-20190923162816-aa69164e4478
	google.golang.org/grpc v1.22.0
)

replace cs.utexas.edu/zjia/faas => /src/nightcore/worker/golang
