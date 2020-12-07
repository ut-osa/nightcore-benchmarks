#include "../utils.h"
#include "../ThriftSeverEventHandler.h"
#include "../ClientPool.h"
#include "../MCClient.h"
#include "ComposeReviewHandler.h"

using namespace media_service;

static json config_json;
static ClientPool<MCClient>* mc_client_pool;

int faas_init() {
  init_logger();

  SetUpTracer("config/jaeger-config.yml", "compose-review-service");

  if (load_config(&config_json) != 0) {
    return -1;
  }

  std::string mc_addr = config_json["compose-review-memcached"]["addr"];
  int mc_port = config_json["compose-review-memcached"]["port"];
  mc_client_pool = new ClientPool<MCClient>("compose-review-memcached", mc_addr, mc_port, 1, 128, 1000);

    return 0;
}

int faas_create_func_worker(void* caller_context,
                            faas_invoke_func_fn_t invoke_func_fn,
                            faas_append_output_fn_t append_output_fn,
                            void** worker_handle) {
    FaasWorker* faas_worker = new FaasWorker(
        caller_context, invoke_func_fn, append_output_fn);

  std::string review_storage_addr =
      config_json["review-storage-service"]["addr"];
  int review_storage_port = config_json["review-storage-service"]["port"];
  std::string review_storage_http_path = config_json["review-storage-service"]["http_path"];

  std::string user_review_addr = config_json["user-review-service"]["addr"];
  int user_review_port = config_json["user-review-service"]["port"];
  std::string user_review_http_path = config_json["user-review-service"]["http_path"];

  std::string movie_review_addr = config_json["movie-review-service"]["addr"];
  int movie_review_port = config_json["movie-review-service"]["port"];
  std::string movie_review_http_path = config_json["movie-review-service"]["http_path"];

  auto compose_client_pool = new ClientPool<ThriftClient<ReviewStorageServiceClient>>(
      "compose-review-service", review_storage_addr, review_storage_port, 0, 128, 1000, "ReviewStorageService", faas_worker,
      "ComposeReviewService", "ReviewStorageService");
  auto user_client_pool = new ClientPool<ThriftClient<UserReviewServiceClient>>(
      "user-review-service", user_review_addr, user_review_port, 0, 128, 1000, "UserReviewService", faas_worker,
      "ComposeReviewService", "UserReviewService");
  auto movie_client_pool = new ClientPool<ThriftClient<MovieReviewServiceClient>>(
      "movie-review-service", movie_review_addr, movie_review_port, 0, 128, 1000, "MovieReviewService", faas_worker,
      "ComposeReviewService", "MovieReviewService");

  faas_worker->SetProcessor(
      std::make_shared<ComposeReviewServiceProcessor>(
          std::make_shared<ComposeReviewHandler>(
              mc_client_pool,
              compose_client_pool,
              user_client_pool,
              movie_client_pool)));
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
