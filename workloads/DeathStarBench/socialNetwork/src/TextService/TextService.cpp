#include "../utils.h"
#include "../ThriftSeverEventHandler.h"
#include "TextHandler.h"

using namespace social_network;

static json config_json;

int faas_init() {
    init_logger();
    SetUpTracer("config/jaeger-config.yml", "post-storage-service");

    if (load_config(&config_json) != 0) {
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

    std::string compose_addr = config_json["compose-post-service"]["addr"];
    int compose_port = config_json["compose-post-service"]["port"];

    std::string url_addr = config_json["url-shorten-service"]["addr"];
    int url_port = config_json["url-shorten-service"]["port"];

    std::string user_mention_addr = config_json["user-mention-service"]["addr"];
    int user_mention_port = config_json["user-mention-service"]["port"];

    auto compose_client_pool = new ClientPool<ThriftClient<ComposePostServiceClient>>(
        "compose-post", compose_addr, compose_port, 0, config_json["text-service"]["compose_client_pool_size"],
        1000, "ComposePostService", faas_worker, "TextService", "ComposePostService");

    auto url_client_pool = new ClientPool<ThriftClient<UrlShortenServiceClient>>(
        "url-shorten-service", url_addr, url_port, 0, config_json["text-service"]["url_client_pool_size"],
        1000, "UrlShortenService", faas_worker, "TextService", "UrlShortenService");

    auto user_mention_pool = new ClientPool<ThriftClient<UserMentionServiceClient>>(
        "user-mention-service", user_mention_addr,
        user_mention_port, 0, config_json["text-service"]["user_mention_pool_size"],
        1000, "UserMentionService", faas_worker, "TextService", "UserMentionService");

    faas_worker->SetProcessor(std::make_shared<TextServiceProcessor>(
            std::make_shared<TextHandler>(
                compose_client_pool,
                url_client_pool,
                user_mention_pool)));
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
