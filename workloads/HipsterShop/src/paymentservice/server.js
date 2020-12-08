// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const path = require('path');
const grpc = require('grpc');
const pino = require('pino');
const protoLoader = require('@grpc/proto-loader');
const faas = require('faas');
const {MongoClient} = require('mongodb');

const charge = require('./charge');

const logger = pino({
  name: 'paymentservice-server',
  messageKey: 'message',
  changeLevelName: 'severity',
  useLevelLabels: true
});

if (process.env.DISABLE_LOGGING == 1) {
  logger.level = 'silent';
}

class HipsterShopServer {
  constructor (protoRoot) {
    this.packages = {
      hipsterShop: this.loadProto(path.join(protoRoot, 'demo.proto'))
    };
  }

  /**
   * Handler for PaymentService.Charge.
   * @param {*} call  { ChargeRequest }
   * @param {*} callback  fn(err, ChargeResponse)
   */
  static ChargeServiceHandler (dbCollection, call, callback) {
    try {
      logger.info(`PaymentService#Charge invoked with request ${JSON.stringify(call.request)}`);
      const response = charge(call.request);
      const transactionId = response.transaction_id;
      dbCollection.insert({ 'transactionId': transactionId }, function (err, result) {
        if (err) {
          callback(err);
        } else {
          callback(null, response);
        }
      });
    } catch (err) {
      console.warn(err);
      callback(err);
    }
  }

  start() {
    const hipsterShopPackage = this.packages.hipsterShop.hipstershop;
  
    const dbUrl = 'mongodb://' + process.env['PAYMENT_DB_ADDR'] + '/payment';
    MongoClient.connect(dbUrl, { minSize: 16, poolSize: 64 }, function (err, client) {
      if (err) {
        console.error('Failed to connect to db', err);
        process.abort();
      }
      client.db('payment').createCollection('transactions', function (err, collection) {
        if (err) {
          console.error('Failed to create collection');
          process.abort();
        }

        faas.serveGrpcService(
          hipsterShopPackage.PaymentService.service,
          {
            charge: function (call, callback) {
              HipsterShopServer.ChargeServiceHandler(collection, call, callback);
            }
          }
        );
      });
    });
  }

  loadProto (path) {
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
}

module.exports = HipsterShopServer;
