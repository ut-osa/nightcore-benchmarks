#!/bin/bash
BASE_DIR=`realpath $(dirname $0)`
ROOT_DIR=`realpath $BASE_DIR/../..`

HELPER_SCRIPT=$ROOT_DIR/scripts/exp_helper

$HELPER_SCRIPT start-machines

ALL_QPS="1000 1500 1800 2000"
for qps in $ALL_QPS; do
    $BASE_DIR/run_once_write.sh write_qps${qps} $qps
    sleep 30
done

ALL_QPS="4000 4500 5000 5500"
for qps in $ALL_QPS; do
    $BASE_DIR/run_once_mixed.sh mixed_qps${qps} $qps
    sleep 30
done

$HELPER_SCRIPT stop-machines
