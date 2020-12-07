mkdir -p build
go build -o build/frontend -ldflags="-s -w" ./cmd/frontend
go build -o build/geo -ldflags="-s -w" ./cmd/geo
go build -o build/profile -ldflags="-s -w" ./cmd/profile
go build -o build/rate -ldflags="-s -w" ./cmd/rate
go build -o build/recommendation -ldflags="-s -w" ./cmd/recommendation
go build -o build/reservation -ldflags="-s -w" ./cmd/reservation
go build -o build/search -ldflags="-s -w" ./cmd/search
go build -o build/user -ldflags="-s -w" ./cmd/user
