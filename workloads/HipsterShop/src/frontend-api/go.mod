module cs.utexas.edu/zjia/hipster-frontend

go 1.14

require (
	cs.utexas.edu/zjia/faas v0.0.0
	github.com/golang/protobuf v1.4.1
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.6.2
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.0.6
	golang.org/x/net v0.0.0-20190311183353-d8887717615a
	google.golang.org/grpc v1.27.0
	google.golang.org/protobuf v1.25.0
)

replace cs.utexas.edu/zjia/faas => /src/nightcore/worker/golang
