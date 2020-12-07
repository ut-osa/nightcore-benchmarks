#include "../utils.h"
#include "../ThriftSeverEventHandler.h"
#include "../ClientPool.h"
#include "../MCClient.h"
#include "../utils_mongodb.h"
#include "MovieIdHandler.h"

using namespace media_service;

static json config_json;
static mongoc_client_pool_t* mongodb_client_pool;
static ClientPool<MCClient>* mc_client_pool;

int faas_init() {
  init_logger();

  SetUpTracer("config/jaeger-config.yml", "movie-id-service");

  if (load_config(&config_json) != 0) {
    return -1;
  }

  std::string mc_addr = config_json["movie-id-memcached"]["addr"];
  int mc_port = config_json["movie-id-memcached"]["port"];
  mc_client_pool = new ClientPool<MCClient>("movie-id-memcached", mc_addr, mc_port, 1, 128, 1000);
  mongodb_client_pool =
      init_mongodb_client_pool(config_json, "movie-id", 128);

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
    r = CreateIndex(mongodb_client, "movie-id", "movie_id", true);
    r = CreateIndex(mongodb_client, "movie-id", "title", true);
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

  std::string compose_addr = config_json["compose-review-service"]["addr"];
  int compose_port = config_json["compose-review-service"]["port"];
  std::string compose_http_path = config_json["compose-review-service"]["http_path"];
  std::string rating_addr = config_json["rating-service"]["addr"];
  int rating_port = config_json["rating-service"]["port"];
  std::string rating_http_path = config_json["rating-service"]["http_path"];
        
  auto compose_client_pool = new ClientPool<ThriftClient<ComposeReviewServiceClient>>(
      "compose-review-client", compose_addr, compose_port, 0, 128, 1000, "ComposeReviewService", faas_worker,
      "MovieIdService", "ComposeReviewService");
  auto rating_client_pool = new ClientPool<ThriftClient<RatingServiceClient>>(
      "rating-client", rating_addr, rating_port, 0, 128, 1000, "RatingService", faas_worker,
      "MovieIdService", "RatingService");

  faas_worker->SetProcessor(
      std::make_shared<MovieIdServiceProcessor>(
      std::make_shared<MovieIdHandler>(
              mc_client_pool, mongodb_client_pool,
              compose_client_pool, rating_client_pool)));
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

