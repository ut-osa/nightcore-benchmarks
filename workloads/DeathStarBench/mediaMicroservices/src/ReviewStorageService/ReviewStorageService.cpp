#include "../utils.h"
#include "../utils_mongodb.h"
#include "../ThriftSeverEventHandler.h"
#include "../ClientPool.h"
#include "../MCClient.h"
#include "ReviewStorageHandler.h"

using namespace media_service;

static json config_json;
static mongoc_client_pool_t* mongodb_client_pool;
static ClientPool<MCClient>* mc_client_pool;


int faas_init() {
  init_logger();

  SetUpTracer("config/jaeger-config.yml", "review-storage-service");

  if (load_config(&config_json) != 0) {
    return -1;
  }

  int port = 9090;

  std::string mc_addr = config_json["review-storage-memcached"]["addr"];
  int mc_port = config_json["review-storage-memcached"]["port"];
  mc_client_pool = new ClientPool<MCClient>("review-storage-memcached", mc_addr, mc_port, 1, 128, 1000);
  mongodb_client_pool = init_mongodb_client_pool(config_json, "review-storage",
      MONGODB_POOL_MAX_SIZE);

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

  faas_worker->SetProcessor(
      std::make_shared<ReviewStorageServiceProcessor>(
          std::make_shared<ReviewStorageHandler>(
              mc_client_pool, mongodb_client_pool)));
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
