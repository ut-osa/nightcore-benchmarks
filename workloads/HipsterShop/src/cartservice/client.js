/*
 *
 * Copyright 2015 gRPC authors.
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
 *
 */
const path = require('path');
const grpc = require('grpc');
const pino = require('pino');

const PROTO_PATH = path.join(__dirname, './proto/demo.proto');
const PORT = 7000;

const shopProto = grpc.load(PROTO_PATH).hipstershop;
const client = new shopProto.CartService(`localhost:${PORT}`,
  grpc.credentials.createInsecure());

const logger = pino({
  name: 'cartservice-client',
  messageKey: 'message',
  changeLevelName: 'severity',
  useLevelLabels: true
});

client.addItem({
  user_id: 'XXX',
  item: { product_id: 'p1', quantity: 5 }
}, (err, response) => {
  if (err) {
    logger.error(`Error in addItem: ${err}`);
    return;
  }
  client.getCart({user_id: 'XXX'}, (err, response) => {
    if (err) {
      logger.error(`Error in getCart: ${err}`);
    }
    logger.info(JSON.stringify(response));
  });
});
