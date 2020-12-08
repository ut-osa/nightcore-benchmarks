FROM zjia/nightcore:asplos-ae as nightcore
FROM golang:1.14-buster as builder

COPY --from=nightcore /src/nightcore       /src/nightcore

WORKDIR /src
COPY . .
RUN go build -gcflags='-N -l' -o main .

FROM debian:buster-slim

COPY --from=builder   /src/main            /frontend/server
COPY --from=nightcore /nightcore/launcher  /nightcore/launcher

ENTRYPOINT [ "/nightcore/launcher", \
             "--fprocess=/frontend/server", \
             "--fprocess_working_dir=/frontend", \
             "--fprocess_mode=go" ]
