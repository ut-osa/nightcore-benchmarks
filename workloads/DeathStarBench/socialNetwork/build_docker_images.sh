#!/bin/bash

SCRIPT_DIR=`dirname $0`

(cd $SCRIPT_DIR/docker/thrift-microservice-deps/cpp ; docker build -t zjia/thrift-microservice-deps:xenial --build-arg NUM_CPUS=`nproc` .)
(cd $SCRIPT_DIR ; docker build -t zjia/social-network-microservices --build-arg NUM_CPUS=`nproc` .)
(cd $SCRIPT_DIR/docker/media-frontend ; docker build -t zjia/media-frontend:xenial -f xenial/Dockerfile --build-arg NUM_CPUS=`nproc` .)
(cd $SCRIPT_DIR/docker/openresty-thrift ; docker build -t zjia/openresty-thrift:xenial -f xenial/Dockerfile --build-arg NUM_CPUS=`nproc` .)