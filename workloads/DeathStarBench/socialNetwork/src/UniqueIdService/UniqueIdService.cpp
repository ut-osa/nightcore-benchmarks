/*
 * 64-bit Unique Id Generator
 *
 * ------------------------------------------------------------------------
 * |0| 11 bit machine ID |      40-bit timestamp         | 12-bit counter |
 * ------------------------------------------------------------------------
 *
 * 11-bit machine Id code by hasing the MAC address
 * 40-bit UNIX timestamp in millisecond precision with custom epoch
 * 12 bit counter which increases monotonically on single process
 *
 */

#include "../utils.h"
#include "../ThriftSeverEventHandler.h"
#include "UniqueIdHandler.h"

using namespace social_network;

static json config_json;
static std::string machine_id;
static std::mutex* thread_lock;

int faas_init() {
    init_logger();
    SetUpTracer("config/jaeger-config.yml", "post-storage-service");

    if (load_config(&config_json) != 0) {
        return -1;
    }

  if (GetMachineId(&machine_id) != 0) {
    return -1;
  }

  thread_lock = new std::mutex;

    return 0;
}

int faas_create_func_worker(void* caller_context,
                            faas_invoke_func_fn_t invoke_func_fn,
                            faas_append_output_fn_t append_output_fn,
                            void** worker_handle) {
    FaasWorker* faas_worker = new FaasWorker(
        caller_context, invoke_func_fn, append_output_fn);

  std::string compose_post_addr = config_json["compose-post-service"]["addr"];
  int compose_post_port = config_json["compose-post-service"]["port"];
  std::string compose_post_http_path = config_json["compose-post-service"]["http_path"];

  auto compose_post_client_pool = new ClientPool<ThriftClient<ComposePostServiceClient>>(
      "compose-post", compose_post_addr, compose_post_port, 0, config_json["unique-id-service"]["compose_post_client_pool_size"],
      1000, "ComposePostService", faas_worker, "UniqueIdService", "ComposePostService");

    faas_worker->SetProcessor(std::make_shared<UniqueIdServiceProcessor>(
          std::make_shared<UniqueIdHandler>(
              thread_lock, machine_id, compose_post_client_pool)));
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
