#!/bin/bash
BASE_DIR=`realpath $(dirname $0)`
ROOT_DIR=`realpath $BASE_DIR/../..`

EXP_DIR=$BASE_DIR/results/$1
QPS=$2

SRC_DIR=$ROOT_DIR/workloads/DeathStarBench/mediaMicroservices
HELPER_SCRIPT=$ROOT_DIR/scripts/exp_helper
WRK_BIN=/usr/local/bin/wrk
WRK_SCRIPT=compose-review.lua

MANAGER_HOST=`$HELPER_SCRIPT get-docker-manager-host --base-dir=$BASE_DIR`
CLIENT_HOST=`$HELPER_SCRIPT get-client-host --base-dir=$BASE_DIR`
ENTRY_HOST=`$HELPER_SCRIPT get-service-host --base-dir=$BASE_DIR --service=nginx-thrift`
MONGO_HOST=`$HELPER_SCRIPT get-service-host --base-dir=$BASE_DIR --service=media-mongodb`
GATEWAY_HOST=`$HELPER_SCRIPT get-service-host --base-dir=$BASE_DIR --service=nightcore-gateway`
ALL_HOSTS=`$HELPER_SCRIPT get-all-server-hosts --base-dir=$BASE_DIR`

ALL_ENGINE_NODES="nightcore-mm-middle1 nightcore-mm-middle2 nightcore-mm-middle3 nightcore-mm-middle4"

$HELPER_SCRIPT generate-docker-compose --base-dir=$BASE_DIR
scp -q $BASE_DIR/docker-compose.yml $MANAGER_HOST:~
scp -q $BASE_DIR/docker-compose-placement.yml $MANAGER_HOST:~

ssh -q $MANAGER_HOST -- docker stack rm media-microservices
sleep 20
ssh -q $ENTRY_HOST -- sudo rm -rf /tmp/mediaMicroservices
ssh -q $ENTRY_HOST -- mkdir -p /tmp/mediaMicroservices
ssh -q $MONGO_HOST -- sudo rm -rf /mnt/inmem/db
ssh -q $MONGO_HOST -- sudo mkdir -p /mnt/inmem/db

for host in $ALL_HOSTS; do
    scp -q $BASE_DIR/nightcore_config.json  $host:/tmp/nightcore_config.json
    scp -q $BASE_DIR/service-config.json    $host:/tmp/service-config.json
done

for name in $ALL_ENGINE_NODES; do
    HOST=`$HELPER_SCRIPT get-host --base-dir=$BASE_DIR --machine-name=$name`
    scp -q $BASE_DIR/run_launcher $HOST:/tmp/run_launcher
    ssh -q $HOST -- sudo rm -rf /mnt/inmem/nightcore
    ssh -q $HOST -- sudo mkdir -p /mnt/inmem/nightcore
    ssh -q $HOST -- sudo mkdir -p /mnt/inmem/nightcore/output /mnt/inmem/nightcore/ipc
    ssh -q $HOST -- sudo cp /tmp/run_launcher /mnt/inmem/nightcore/run_launcher
    ssh -q $HOST -- sudo cp /tmp/nightcore_config.json /mnt/inmem/nightcore/func_config.json
done

rsync -arq $SRC_DIR/nginx-web-server    $ENTRY_HOST:/tmp/mediaMicroservices
rsync -arq $SRC_DIR/gen-lua             $ENTRY_HOST:/tmp/mediaMicroservices
rsync -arq $SRC_DIR/docker              $ENTRY_HOST:/tmp/mediaMicroservices

ssh -q $ENTRY_HOST -- sed -i '"s/worker_processes  4/worker_processes  8/g"' \
    /tmp/mediaMicroservices/nginx-web-server/conf/nginx.conf

ssh -q $MANAGER_HOST -- docker stack deploy \
    -c ~/docker-compose.yml -c ~/docker-compose-placement.yml media-microservices
sleep 60

for name in $ALL_ENGINE_NODES; do
    HOST=`$HELPER_SCRIPT get-host --base-dir=$BASE_DIR --machine-name=$name`
    ENGINE_CONTAINER_ID=`$HELPER_SCRIPT get-container-id --service faas-engine --machine-name=$name`
    echo 4096 | ssh -q $HOST -- sudo tee /sys/fs/cgroup/cpu,cpuacct/docker/$ENGINE_CONTAINER_ID/cpu.shares
done

scp -q $SRC_DIR/scripts/register_users.sh    $CLIENT_HOST:~
scp -q $SRC_DIR/scripts/write_movie_info.py  $CLIENT_HOST:~
scp -q $SRC_DIR/wrk2_scripts/$WRK_SCRIPT     $CLIENT_HOST:~
scp -qr $SRC_DIR/datasets/tmdb               $CLIENT_HOST:~

ssh -q $CLIENT_HOST -- ~/register_users.sh $ENTRY_HOST
ssh -q $CLIENT_HOST -- python3 ~/write_movie_info.py -x $ENTRY_HOST \
    -c ~/tmdb/casts.json -m ~/tmdb/movies.json

sleep 10

rm -rf $EXP_DIR
mkdir -p $EXP_DIR

ssh -q $CLIENT_HOST -- $WRK_BIN -t 4 -c 96 -d 30 -L -U \
    -s ~/$WRK_SCRIPT \
    http://$ENTRY_HOST:8080 -R $QPS 2>/dev/null >$EXP_DIR/wrk_warmup.log

sleep 5

ssh -q $CLIENT_HOST -- $WRK_BIN -t 4 -c 96 -d 150 -L -U \
    -s ~/$WRK_SCRIPT \
    http://$ENTRY_HOST:8080 -R $QPS 2>/dev/null >$EXP_DIR/wrk.log

$HELPER_SCRIPT collect-container-logs --base-dir=$BASE_DIR --log-path=$EXP_DIR/logs

for name in $ALL_ENGINE_NODES; do
    HOST=`$HELPER_SCRIPT get-host --base-dir=$BASE_DIR --machine-name=$name`
    mkdir $EXP_DIR/logs/func_worker_$name
    rsync -arq $HOST:/mnt/inmem/nightcore/output/* $EXP_DIR/logs/func_worker_$name
done
