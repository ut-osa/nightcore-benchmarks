#!/usr/bin/python
#
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import faas
import random
import time

import demo_pb2
import demo_pb2_grpc

# from logger import getJSONLogger
# logger = getJSONLogger('recommendationservice-server')


async def ListRecommendations(request, context):
    max_responses = 5
    # fetch list of products from product catalog stub
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(context.grpc_channel)
    cat_response = await product_catalog_stub.ListProducts(demo_pb2.Empty())
    product_ids = [x.id for x in cat_response.products]
    filtered_products = list(set(product_ids)-set(request.product_ids))
    num_products = len(filtered_products)
    num_return = min(max_responses, num_products)
    # sample list of indicies to return
    indices = random.sample(range(num_products), num_return)
    # fetch product ids from indices
    prod_list = [filtered_products[i] for i in indices]
    # logger.info("[Recv ListRecommendations] product_ids={}".format(prod_list))
    # build and return response
    response = demo_pb2.ListRecommendationsResponse()
    response.product_ids.extend(prod_list)
    return response


def handler_factory(func_name):
    async def handler(ctx, method, request_bytes):
        if method == 'ListRecommendations':
            request = demo_pb2.ListRecommendationsRequest.FromString(request_bytes)
            response = await ListRecommendations(request, ctx)
            return demo_pb2.ListRecommendationsResponse.SerializeToString(response)
        else:
            raise faas.Error('Unknown method: {}'.format(method))
    
    if func_name == 'hipstershop.RecommendationService':
        return handler
    else:
        sys.stderr.write('Unknown function: {}\n'.format(func_name))
        sys.exit(1)


if __name__ == "__main__":
    faas.serve_forever(handler_factory)
