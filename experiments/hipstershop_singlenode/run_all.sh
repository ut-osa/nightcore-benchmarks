#!/bin/bash
BASE_DIR=`realpath $(dirname $0)`
ROOT_DIR=`realpath $BASE_DIR/../..`

HELPER_SCRIPT=$ROOT_DIR/scripts/exp_helper

$HELPER_SCRIPT start-machines

ALL_QPS="2000 2200 2500 2800"
for qps in $ALL_QPS; do
    sleep 30
    $BASE_DIR/run_once.sh qps${qps} $qps
done

$HELPER_SCRIPT stop-machines
