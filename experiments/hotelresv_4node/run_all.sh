#!/bin/bash
BASE_DIR=`realpath $(dirname $0)`
ROOT_DIR=`realpath $BASE_DIR/../..`

HELPER_SCRIPT=$ROOT_DIR/scripts/exp_helper

$HELPER_SCRIPT start-machines

ALL_QPS="8000 10000 12000"
for qps in $ALL_QPS; do
    sleep 30
    $BASE_DIR/run_once.sh qps${qps} $qps
done

$HELPER_SCRIPT stop-machines
