#include "../utils.h"
#include "../ThriftSeverEventHandler.h"
#include "ComposePostHandler.h"


using namespace social_network;

static json config_json;
static ClientPool<RedisClient>* redis_client_pool;
static ClientPool<RabbitmqClient>* rabbitmq_client_pool;

int faas_init() {
    init_logger();
    SetUpTracer("config/jaeger-config.yml", "post-storage-service");

    if (load_config(&config_json) != 0) {
        return -1;
    }

  int redis_port = config_json["compose-post-redis"]["port"];
  std::string redis_addr = config_json["compose-post-redis"]["addr"];
  int rabbitmq_port = config_json["write-home-timeline-rabbitmq"]["port"];
  std::string rabbitmq_addr =
      config_json["write-home-timeline-rabbitmq"]["addr"];

  redis_client_pool = new ClientPool<RedisClient>("redis", redis_addr, redis_port,
                                            0, config_json["compose-post-service"]["redis_client_pool_size"], 1000);
  rabbitmq_client_pool = new ClientPool<RabbitmqClient>("rabbitmq", rabbitmq_addr,
      rabbitmq_port, 0, config_json["compose-post-service"]["rabbitmq_client_pool_size"],
      1000, "", nullptr, "ComposePostService", "WriteHomeTimelineRabbitMQ");

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

  int user_timeline_port = config_json["user-timeline-service"]["port"];
  std::string user_timeline_addr = config_json["user-timeline-service"]["addr"];

  auto post_storage_client_pool = new ClientPool<ThriftClient<PostStorageServiceClient>>("post-storage-client", post_storage_addr,
                               post_storage_port, 0, config_json["compose-post-service"]["post_storage_client_pool_size"],
                               1000, "PostStorageService", faas_worker, "ComposePostService", "PostStorageService");
  auto user_timeline_client_pool = new ClientPool<ThriftClient<UserTimelineServiceClient>>("user-timeline-client", user_timeline_addr,
                                user_timeline_port, 0, config_json["compose-post-service"]["user_timeline_client_pool_size"],
                                1000, "UserTimelineService", faas_worker, "ComposePostService", "UserTimelineService");

    faas_worker->SetProcessor(std::make_shared<ComposePostServiceProcessor>(
          std::make_shared<ComposePostHandler>(
              redis_client_pool,
              post_storage_client_pool,
              user_timeline_client_pool,
              rabbitmq_client_pool)));
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