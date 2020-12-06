#include "../faas/worker_v1_interface.h"

#include "../ClientPool.h"
#include "../RedisClient.h"
#include "../logger.h"
#include "../tracing.h"
#include "../utils.h"
#include "../utils_mongodb.h"
#include "../../gen-cpp/social_network_types.h"
#include "../FaasWorker.h"
#include "UserTimelineHandler.h"

using namespace social_network;

static mongoc_client_pool_t* mongodb_client_pool;
static ClientPool<RedisClient>* redis_client_pool;
static json config_json;

int faas_init() {
    init_logger();
    SetUpTracer("config/jaeger-config.yml", "post-storage-service");

    if (load_config(&config_json) != 0) {
        return -1;
    }

    std::string redis_addr =
        config_json["user-timeline-redis"]["addr"];
    int redis_port = config_json["user-timeline-redis"]["port"];

    mongodb_client_pool = init_mongodb_client_pool(
        config_json, "user-timeline", 128);

    redis_client_pool = new ClientPool<RedisClient>("user-timeline-redis", 
        redis_addr, redis_port, 0, config_json["user-timeline-service"]["redis_client_pool_size"], 1000);

    if (mongodb_client_pool == nullptr) {
      return -1;
    }

    mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
    if (!mongodb_client) {
      LOG(fatal) << "Failed to pop mongoc client";
      return -1;
    }
    bool r = false;
    while (!r) {
      r = CreateIndex(mongodb_client, "user-timeline", "user_id", true);
      if (!r) {
        LOG(error) << "Failed to create mongodb index, try again";
        sleep(1);
      }
    }
    mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

    return 0;
}

int faas_create_func_worker(void* caller_context,
                            faas_invoke_func_fn_t invoke_func_fn,
                            faas_append_output_fn_t append_output_fn,
                            void** worker_handle) {
    FaasWorker* faas_worker = new FaasWorker(
        caller_context, invoke_func_fn, append_output_fn);

    int post_storage_port = config_json["post-storage-service"]["port"];
    std::string post_storage_addr = config_json["post-storage-service"]["addr"];
    auto post_storage_client_pool = new ClientPool<ThriftClient<PostStorageServiceClient>>(
        "post-storage-client", post_storage_addr, post_storage_port, 0,
        config_json["user-timeline-service"]["post_storage_client_pool_size"],
        1000, "PostStorageService", faas_worker, "UserTimelineService", "PostStorageService");

    faas_worker->SetProcessor(std::make_shared<UserTimelineServiceProcessor>(
          std::make_shared<UserTimelineHandler>(
              redis_client_pool, mongodb_client_pool,
              post_storage_client_pool)));
    *worker_handle = faas_worker;
    return 0;
}

int faas_destroy_func_worker(void* worker_handle) {
    FaasWorker* faas_worker = reinterpret_cast<FaasWorker*>(worker_handle);
    delete faas_worker;
    return 0;
}

int faas_func_call(void* worker_handle,
                   const char* input, size_t input_length) {
    FaasWorker* faas_worker = reinterpret_cast<FaasWorker*>(worker_handle);
    bool success = faas_worker->Process(input, input_length);
    return success ? 0 : -1;
}