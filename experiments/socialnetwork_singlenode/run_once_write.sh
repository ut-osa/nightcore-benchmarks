#!/bin/bash
BASE_DIR=`realpath $(dirname $0)`
ROOT_DIR=`realpath $BASE_DIR/../..`

EXP_DIR=$BASE_DIR/results/$1
QPS=$2

SRC_DIR=$ROOT_DIR/workloads/DeathStarBench/socialNetwork
HELPER_SCRIPT=$ROOT_DIR/scripts/exp_helper
WRK_BIN=/usr/local/bin/wrk
WRK_SCRIPT=compose-post.lua

MANAGER_HOST=`$HELPER_SCRIPT get-docker-manager-host --base-dir=$BASE_DIR`
CLIENT_HOST=`$HELPER_SCRIPT get-client-host --base-dir=$BASE_DIR`
ENTRY_HOST=`$HELPER_SCRIPT get-service-host --base-dir=$BASE_DIR --service=nginx-thrift`
MONGO_HOST=`$HELPER_SCRIPT get-service-host --base-dir=$BASE_DIR --service=socialnetwork-mongodb`
ENGINE_HOST=`$HELPER_SCRIPT get-service-host --base-dir=$BASE_DIR --service=nightcore-engine`
GATEWAY_HOST=`$HELPER_SCRIPT get-service-host --base-dir=$BASE_DIR --service=nightcore-gateway`
ALL_HOSTS=`$HELPER_SCRIPT get-all-server-hosts --base-dir=$BASE_DIR`

$HELPER_SCRIPT generate-docker-compose --base-dir=$BASE_DIR
scp -q $BASE_DIR/docker-compose-write.yml $MANAGER_HOST:~
scp -q $BASE_DIR/docker-compose-placement.yml $MANAGER_HOST:~

ssh -q $MANAGER_HOST -- docker stack rm socialnetwork
sleep 20
ssh -q $ENTRY_HOST -- sudo rm -rf /tmp/socialNetwork
ssh -q $ENTRY_HOST -- mkdir -p /tmp/socialNetwork
ssh -q $MONGO_HOST -- sudo rm -rf /mnt/inmem/db
ssh -q $MONGO_HOST -- sudo mkdir -p /mnt/inmem/db
ssh -q $ENGINE_HOST -- sudo rm -rf /mnt/inmem/nightcore
ssh -q $ENGINE_HOST -- sudo mkdir -p /mnt/inmem/nightcore
ssh -q $ENGINE_HOST -- sudo mkdir -p /mnt/inmem/nightcore/output /mnt/inmem/nightcore/ipc

for host in $ALL_HOSTS; do
    scp -q $BASE_DIR/nightcore_config.json  $host:/tmp/nightcore_config.json
    scp -q $BASE_DIR/service-config.json    $host:/tmp/service-config.json
done

scp -q $BASE_DIR/run_launcher $ENGINE_HOST:/tmp/run_launcher
ssh -q $ENGINE_HOST -- sudo cp /tmp/run_launcher /mnt/inmem/nightcore/run_launcher
ssh -q $ENGINE_HOST -- sudo cp /tmp/nightcore_config.json /mnt/inmem/nightcore/func_config.json

rsync -arq $SRC_DIR/nginx-web-server    $ENTRY_HOST:/tmp/socialNetwork
rsync -arq $SRC_DIR/media-frontend      $ENTRY_HOST:/tmp/socialNetwork
rsync -arq $SRC_DIR/gen-lua             $ENTRY_HOST:/tmp/socialNetwork
rsync -arq $SRC_DIR/docker              $ENTRY_HOST:/tmp/socialNetwork

ssh -q $MANAGER_HOST -- SRC_DIR=/mnt/efs/workspace/DeathStarBench-faas/socialNetwork \
    docker stack deploy \
    -c ~/docker-compose-write.yml -c ~/docker-compose-placement.yml socialnetwork
sleep 60

ENGINE_CONTAINER_ID=`$HELPER_SCRIPT get-container-id --service nightcore-engine`
echo 4096 | ssh -q $ENGINE_HOST -- sudo tee /sys/fs/cgroup/cpu,cpuacct/docker/$ENGINE_CONTAINER_ID/cpu.shares

scp -q $SRC_DIR/scripts/init_social_graph.py                         $CLIENT_HOST:~
scp -q $SRC_DIR/wrk2_scripts/$WRK_SCRIPT                             $CLIENT_HOST:~
scp -q $SRC_DIR/datasets/social-graph/socfb-Reed98/socfb-Reed98.mtx  $CLIENT_HOST:~

ssh -q $CLIENT_HOST -- python3 ~/init_social_graph.py $ENTRY_HOST ~/socfb-Reed98.mtx

sleep 10

rm -rf $EXP_DIR
mkdir -p $EXP_DIR

ssh -q $CLIENT_HOST -- $WRK_BIN -t 4 -c 48 -d 30 -L -U \
    -s ~/$WRK_SCRIPT \
    http://$ENTRY_HOST:8080 -R $QPS 2>/dev/null >$EXP_DIR/wrk_warmup.log

sleep 5

ssh -q $CLIENT_HOST -- $WRK_BIN -t 4 -c 48 -d 150 -L -U \
    -s ~/$WRK_SCRIPT \
    http://$ENTRY_HOST:8080 -R $QPS 2>/dev/null >$EXP_DIR/wrk.log

$HELPER_SCRIPT collect-container-logs --base-dir=$BASE_DIR --log-path=$EXP_DIR/logs

mkdir $EXP_DIR/logs/func_worker
rsync -arq $ENGINE_HOST:/mnt/inmem/nightcore/output/* $EXP_DIR/logs/func_worker
