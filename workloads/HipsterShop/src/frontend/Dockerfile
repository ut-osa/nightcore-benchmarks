FROM golang:1.14-alpine as builder

WORKDIR /src
COPY . .
RUN go build -gcflags='-N -l' -o main .

FROM alpine as release
RUN apk add --no-cache ca-certificates \
    busybox-extras net-tools bind-tools
WORKDIR /frontend
COPY --from=builder /src/main /frontend/server
COPY ./templates ./templates
COPY ./static ./static
EXPOSE 8080
ENTRYPOINT ["/frontend/server"]
