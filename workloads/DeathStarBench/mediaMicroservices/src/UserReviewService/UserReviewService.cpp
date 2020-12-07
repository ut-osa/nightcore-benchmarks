#include "UserReviewHandler.h"
#include "../utils.h"
#include "../ThriftSeverEventHandler.h"
#include "../utils_mongodb.h"

using namespace media_service;

static json config_json;
static ClientPool<RedisClient>* redis_client_pool;
static mongoc_client_pool_t *mongodb_client_pool;
static std::string machine_id;
static std::mutex* thread_lock;

int faas_init() {
  init_logger();

  SetUpTracer("config/jaeger-config.yml", "user-review-service");

  if (load_config(&config_json) != 0) {
    LOG(fatal) << "Cannot open the config file.";
    return -1;
  }

    std::string redis_addr =
      config_json["user-review-redis"]["addr"];
  int redis_port = config_json["user-review-redis"]["port"];

  mongodb_client_pool = init_mongodb_client_pool(config_json, "user-review", 128);
  redis_client_pool = new ClientPool<RedisClient>("user-review-redis",
                                            redis_addr, redis_port, 0, 128, 1000);

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
    r = CreateIndex(mongodb_client, "user-review", "user_id", true);
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
  
    std::string secret = config_json["secret"];

  int review_storage_port = config_json["review-storage-service"]["port"];
  std::string review_storage_addr = config_json["review-storage-service"]["addr"];
  std::string review_storage_http_path = config_json["review-storage-service"]["http_path"];

    auto review_storage_client_pool = new ClientPool<ThriftClient<ReviewStorageServiceClient>>("review-storage-client", review_storage_addr,
                                 review_storage_port, 0, 128, 1000, "ReviewStorageService", faas_worker,
                                 "UserReviewService", "ReviewStorageService");

  faas_worker->SetProcessor(
      std::make_shared<UserReviewServiceProcessor>(
          std::make_shared<UserReviewHandler>(
              redis_client_pool,
              mongodb_client_pool,
              review_storage_client_pool)));
 
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