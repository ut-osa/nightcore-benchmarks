#include "../utils.h"
#include "../ThriftSeverEventHandler.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../RedisClient.h"
#include "RatingHandler.h"

using namespace media_service;

static json config_json;
static ClientPool<RedisClient>* redis_client_pool;

int faas_init() {
  init_logger();

  SetUpTracer("config/jaeger-config.yml", "rating-service");

  if (load_config(&config_json) != 0) {
    return -1;
  }

  std::string redis_addr = config_json["rating-redis"]["addr"];
  int redis_port = config_json["rating-redis"]["port"];

  redis_client_pool = new ClientPool<RedisClient>("rating-redis",
      redis_addr, redis_port, 0, 128, 1000);
  
  
  return 0;
}

int faas_create_func_worker(void* caller_context,
                            faas_invoke_func_fn_t invoke_func_fn,
                            faas_append_output_fn_t append_output_fn,
                            void** worker_handle) {
    FaasWorker* faas_worker = new FaasWorker(
        caller_context, invoke_func_fn, append_output_fn);

  int port = 9090;
  std::string compose_addr = config_json["compose-review-service"]["addr"];
  int compose_port = config_json["compose-review-service"]["port"];
  std::string compose_http_path = config_json["compose-review-service"]["http_path"];

  auto compose_client_pool = new ClientPool<ThriftClient<ComposeReviewServiceClient>>(
      "compose-review-client", compose_addr, compose_port, 0, 128, 1000, "ComposeReviewService", faas_worker,
      "RatingService", "ComposeReviewService");

  faas_worker->SetProcessor(
      std::make_shared<RatingServiceProcessor>(
          std::make_shared<RatingHandler>(
              compose_client_pool, 
              redis_client_pool)));
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
