#include "../utils.h"
#include "../utils_mongodb.h"
#include "../ThriftSeverEventHandler.h"
#include "SocialGraphHandler.h"

using json = nlohmann::json;
using namespace social_network;

static json config_json;
static mongoc_client_pool_t *mongodb_client_pool;
static ClientPool<RedisClient>* redis_client_pool;

int faas_init() {
    init_logger();
    SetUpTracer("config/jaeger-config.yml", "post-storage-service");

    if (load_config(&config_json) != 0) {
        return -1;
    }

  int redis_port = config_json["social-graph-redis"]["port"];
  std::string redis_addr = config_json["social-graph-redis"]["addr"];

  redis_client_pool = new ClientPool<RedisClient>("redis", redis_addr, redis_port,
      0, config_json["social-graph-service"]["redis_client_pool_size"], 1000);

  mongodb_client_pool = init_mongodb_client_pool(config_json, "social-graph", 128);

  if (mongodb_client_pool == nullptr) {
    return EXIT_FAILURE;
  }

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    return EXIT_FAILURE;
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "social-graph", "user_id", true);
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

  std::string user_addr = config_json["user-service"]["addr"];
  int user_port = config_json["user-service"]["port"];
  std::string user_http_path = config_json["user-service"]["http_path"];

  auto user_client_pool = new ClientPool<ThriftClient<UserServiceClient>>(
      "social-graph", user_addr, user_port, 0, config_json["social-graph-service"]["user_client_pool_size"],
      1000, "UserService", faas_worker, "SocialGraphService", "UserService");

    faas_worker->SetProcessor(std::make_shared<SocialGraphServiceProcessor>(
          std::make_shared<SocialGraphHandler>(
              mongodb_client_pool,
              redis_client_pool,
              user_client_pool)));
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
