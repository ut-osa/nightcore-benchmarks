/*
 * Copyright 2018 Google LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const path = require('path');
const grpc = require('grpc');
const pino = require('pino');
const redis = require("redis");
const protoLoader = require('@grpc/proto-loader');
const faas = require('faas');

const MAIN_PROTO_PATH = path.join(__dirname, './proto/demo.proto');

const redisClient = redis.createClient({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: process.env.REDIS_PORT || 6379
});

const shopProto = _loadProto(MAIN_PROTO_PATH).hipstershop;

const logger = pino({
  name: 'cartservice-server',
  messageKey: 'message',
  changeLevelName: 'severity',
  useLevelLabels: true
});

if (process.env.DISABLE_LOGGING == 1) {
  logger.level = 'silent';
}

/**
 * Helper function that loads a protobuf file.
 */
function _loadProto (path) {
  const packageDefinition = protoLoader.loadSync(
    path,
    {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true
    }
  );
  return grpc.loadPackageDefinition(packageDefinition);
}

function addItem (call, callback) {
  logger.info('add item to cart');
  const request = call.request;
  redisClient.lpush(request.user_id, JSON.stringify(request.item), function (err, res) {
    if (err) {
      callback(err);
    }
    callback(null, {});
  });
}

function getCart (call, callback) {
  logger.info('get cart');
  const request = call.request;
  redisClient.lrange(request.user_id, 0, -1, function (err, res) {
    if (err) {
      callback(err);
    }
    callback(null, {
      user_id: request.user_id,
      items: res.map(itemJson => JSON.parse(itemJson)),
    });
  });
}

function emptyCart (call, callback) {
  logger.info('empty cart');
  const request = call.request;
  redisClient.del(request.user_id, function (err, res) {
    if (err) {
      callback(err);
    }
    callback(null, {});
  });
}

/**
 * Starts an RPC server that receives requests for the
 * CurrencyConverter service at the sample server port
 */
function main () {
  faas.serveGrpcService(shopProto.CartService.service, {addItem, getCart, emptyCart});
}

main();
