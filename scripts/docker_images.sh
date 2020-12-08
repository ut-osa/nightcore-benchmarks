#!/bin/bash

ROOT_DIR=`realpath $(dirname $0)/..`

function build_nightcore {
    docker build -t zjia/nightcore:asplos-ae \
        -f $ROOT_DIR/dockerfiles/Dockerfile.nightcore \
        $ROOT_DIR/nightcore
}

function push_nightcore {
    docker push zjia/nightcore:asplos-ae
}

function build_deathstarbench {
    docker build -t zjia/cpp-microservice-deps:asplos-ae \
        --build-arg NUM_CPUS=$(nproc) \
        $ROOT_DIR/workloads/DeathStarBench/socialNetwork/docker/cpp-microservice-deps

    docker build -t zjia/nightcore-socialnetwork:asplos-ae \
        -f $ROOT_DIR/dockerfiles/Dockerfile.socialnetwork \
        --build-arg NUM_CPUS=$(nproc) \
        $ROOT_DIR/workloads/DeathStarBench

    docker build -t zjia/nightcore-moviereview:asplos-ae \
        -f $ROOT_DIR/dockerfiles/Dockerfile.moviereview \
        --build-arg NUM_CPUS=$(nproc) \
        $ROOT_DIR/workloads/DeathStarBench

    docker build -t zjia/nightcore-hotelresv:asplos-ae \
        -f $ROOT_DIR/dockerfiles/Dockerfile.hotelresv \
        $ROOT_DIR/workloads/DeathStarBench
}

function push_deathstarbench {
    docker push zjia/cpp-microservice-deps:asplos-ae
    docker push zjia/nightcore-socialnetwork:asplos-ae
    docker push zjia/nightcore-moviereview:asplos-ae
    docker push zjia/nightcore-hotelresv:asplos-ae
}

HIPSTERSHOP_SERVICES="frontend-api \
productcatalogservice \
shippingservice \
checkoutservice \
currencyservice \
paymentservice \
adservice \
recommendationservice \
cartservice"

function build_hipstershop {
    for service in $HIPSTERSHOP_SERVICES; do
        docker build -t zjia/nightcore-hipstershop:${service}-asplos-ae \
            $ROOT_DIR/workloads/HipsterShop/src/${service}
    done
}

function push_hipstershop {
    for service in $HIPSTERSHOP_SERVICES; do
        docker push zjia/nightcore-hipstershop:${service}-asplos-ae
    done
}

function build {
    build_nightcore
    build_deathstarbench
    build_hipstershop
}

function push {
    push_nightcore
    push_deathstarbench
    push_hipstershop
}

case "$1" in
build)
    build
    ;;
push)
    push
    ;;
esac
