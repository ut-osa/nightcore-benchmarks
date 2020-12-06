#include "nlohmann/json.hpp"

#include "../utils.h"
#include "../utils_mongodb.h"
#include "../ThriftSeverEventHandler.h"
#include "../ClientPool.h"
#include "../MCClient.h"
#include "UserMentionHandler.h"

using namespace social_network;

static json config_json;
static mongoc_client_pool_t* mongodb_client_pool;
static ClientPool<MCClient>* mc_client_pool;

int faas_init() {
    init_logger();
    SetUpTracer("config/jaeger-config.yml", "post-storage-service");

    if (load_config(&config_json) != 0) {
        return -1;
    }

  std::string mc_addr = config_json["user-memcached"]["addr"];
  int mc_port = config_json["user-memcached"]["port"];
  mc_client_pool = new ClientPool<MCClient>("user-memcached", mc_addr, mc_port, 1, 128, 1000);

  mongodb_client_pool = init_mongodb_client_pool(config_json, "user", 128);
  if (mongodb_client_pool == nullptr) {
    return -1;
  }

    return 0;
}

int faas_create_func_worker(void* caller_context,
                            faas_invoke_func_fn_t invoke_func_fn,
                            faas_append_output_fn_t append_output_fn,
                            void** worker_handle) {
    FaasWorker* faas_worker = new FaasWorker(
        caller_context, invoke_func_fn, append_output_fn);

  const std::string compose_post_addr = config_json["compose-post-service"]["addr"];
  int compose_post_port = config_json["compose-post-service"]["port"];

  auto compose_post_client_pool = new ClientPool<ThriftClient<ComposePostServiceClient>>(
      "compose-post", compose_post_addr, compose_post_port, 0, config_json["user-mention-service"]["compose_post_client_pool_size"],
      1000, "ComposePostService", faas_worker, "UserMentionService", "ComposePostService");

    faas_worker->SetProcessor(std::make_shared<UserMentionServiceProcessor>(
          std::make_shared<UserMentionHandler>(
              mc_client_pool, mongodb_client_pool,
              compose_post_client_pool)));
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