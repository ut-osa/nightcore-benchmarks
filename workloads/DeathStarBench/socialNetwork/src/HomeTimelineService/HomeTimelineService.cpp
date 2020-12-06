#include "HomeTimelineHandler.h"
#include "../ClientPool.h"
#include "../RedisClient.h"
#include "../logger.h"
#include "../tracing.h"
#include "../utils.h"
#include "../ThriftSeverEventHandler.h"

using namespace social_network;

static json config_json;
static ClientPool<RedisClient>* redis_client_pool;

int faas_init() {
    init_logger();
    SetUpTracer("config/jaeger-config.yml", "post-storage-service");

    if (load_config(&config_json) != 0) {
        return -1;
    }

  std::string redis_addr =
      config_json["home-timeline-redis"]["addr"];
  int redis_port = config_json["home-timeline-redis"]["port"];

  redis_client_pool = new ClientPool<RedisClient>("home-timeline-redis",
      redis_addr, redis_port, 0, config_json["home-timeline-service"]["redis_client_pool_size"], 1000);

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
  std::string post_storage_http_path = config_json["post-storage-service"]["http_path"];

  auto post_storage_client_pool = new ClientPool<ThriftClient<PostStorageServiceClient>>("post-storage-client", post_storage_addr,
                               post_storage_port, 0, config_json["home-timeline-service"]["post_storage_client_pool_size"],
                               1000, "PostStorageService", faas_worker, "HomeTimelineService", "PostStorageService");

    faas_worker->SetProcessor(std::make_shared<HomeTimelineServiceProcessor>(
          std::make_shared<ReadHomeTimelineHandler>(
              redis_client_pool,
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