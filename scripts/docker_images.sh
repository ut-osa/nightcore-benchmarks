#!/bin/bash

ROOT_DIR=`realpath $(dirname $0)/..`

function build {
    docker build -t zjia/cpp-microservice-deps:asplos-ae \
        --build-arg NUM_CPUS=$(nproc) \
        $ROOT_DIR/workloads/DeathStarBench/socialNetwork/docker/cpp-microservice-deps

    docker build -t zjia/nightcore:asplos-ae \
        -f $ROOT_DIR/dockerfiles/Dockerfile.nightcore \
        $ROOT_DIR/nightcore

    docker build -t zjia/nightcore-socialnetwork:asplos-ae \
        -f $ROOT_DIR/dockerfiles/Dockerfile.socialnetwork \
        --build-arg NUM_CPUS=$(nproc) \
        $ROOT_DIR/workloads/DeathStarBench

    docker build -t zjia/nightcore-moviereview:asplos-ae \
        -f $ROOT_DIR/dockerfiles/Dockerfile.moviereview \
        --build-arg NUM_CPUS=$(nproc) \
        $ROOT_DIR/workloads/DeathStarBench
}

function push {
    docker push zjia/cpp-microservice-deps:asplos-ae
    docker push zjia/nightcore:asplos-ae
    docker push zjia/nightcore-socialnetwork:asplos-ae
    docker push zjia/nightcore-moviereview:asplos-ae
}

case "$1" in
build)
    build
    ;;
push)
    push
    ;;
esac
