#!/bin/bash

ROOT_DIR=`realpath $(dirname $0)/..`

docker build -t zjia/cpp-microservice-deps:bionic \
    -f $ROOT_DIR/dockerfiles/Dockerfile.cpp-microservice-deps \
    --build-arg NUM_CPUS=$(nproc) \
    $ROOT_DIR/dockerfiles 

docker build -t zjia/nightcore:asplos-ae \
    -f $ROOT_DIR/dockerfiles/Dockerfile.nightcore \
    $ROOT_DIR/nightcore

docker build -t zjia/nightcore-socialnetwork:asplos-ae \
    -f $ROOT_DIR/dockerfiles/Dockerfile.socialnetwork \
    --build-arg NUM_CPUS=$(nproc) \
    $ROOT_DIR/workloads/DeathStarBench
