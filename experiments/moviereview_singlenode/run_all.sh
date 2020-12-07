#!/bin/bash
BASE_DIR=`realpath $(dirname $0)`
ROOT_DIR=`realpath $BASE_DIR/../..`

HELPER_SCRIPT=$ROOT_DIR/scripts/exp_helper

$HELPER_SCRIPT start-machines

ALL_QPS="1000 1400 1600 1800"
for qps in $ALL_QPS; do
    $BASE_DIR/run_once.sh qps${qps} $qps
    sleep 30
done

$HELPER_SCRIPT stop-machines
