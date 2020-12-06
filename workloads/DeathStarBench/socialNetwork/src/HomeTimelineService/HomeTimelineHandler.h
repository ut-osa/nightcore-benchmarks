#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_

#include <iostream>
#include <string>
#include <future>

#include <hiredis/hiredis.h>

#include "../../gen-cpp/HomeTimelineService.h"
#include "../../gen-cpp/PostStorageService.h"
#include "../logger.h"
#include "../tracing.h"
#include "../ClientPool.h"
#include "../RedisClient.h"
#include "../ThriftClient.h"

namespace social_network {

class ReadHomeTimelineHandler : public HomeTimelineServiceIf {
 public:
  explicit ReadHomeTimelineHandler(
      ClientPool<RedisClient> *,
      ClientPool<ThriftClient<PostStorageServiceClient>> *);
  ~ReadHomeTimelineHandler() override = default;

  void ReadHomeTimeline(std::vector<Post> &, int64_t, int64_t, int, int,
      const std::map<std::string, std::string> &) override ;

  ClientPool<RedisClient> *_redis_client_pool;
  ClientPool<ThriftClient<PostStorageServiceClient>> *_post_client_pool;
};

ReadHomeTimelineHandler::ReadHomeTimelineHandler(
    ClientPool<RedisClient> *redis_pool,
    ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool) {
  _redis_client_pool = redis_pool;
  _post_client_pool = post_client_pool;
}

void ReadHomeTimelineHandler::ReadHomeTimeline(
    std::vector<Post> & _return,
    int64_t req_id,
    int64_t user_id,
    int start,
    int stop,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "ReadHomeTimeline",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  if (stop <= start || start < 0) {
    return;
  }

  auto redis_client_wrapper = _redis_client_pool->Pop();
  if (!redis_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_REDIS_ERROR;
    se.message = "Cannot connect to Redis server";
    throw se;
  }
  auto redis_client = redis_client_wrapper->GetClient();
  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "RedisFind", {opentracing::ChildOf(&span->context())});
  redis_client.AppendCommand("ZREVRANGE %" PRId64 " %d %d", user_id, start, stop - 1);
  auto post_ids_reply = redis_client.GetReply();
  _redis_client_pool->Push(redis_client_wrapper);
  redis_span->Finish();

  std::vector<int64_t> post_ids;
  auto post_ids_reply_array = post_ids_reply->as_array();
  for (auto &post_id_reply : post_ids_reply_array) {
    post_ids.emplace_back(std::stoul(post_id_reply->as_string()));
  }

  auto post_client_wrapper = _post_client_pool->Pop();
  if (!post_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to post-storage-service";
    throw se;
  }
  auto post_client = post_client_wrapper->GetClient();
  {
    auto rpc_trace_guard = _post_client_pool->StartRpcTrace("ReadPosts", post_client_wrapper);
    try {
      post_client->ReadPosts(_return, req_id, post_ids, writer_text_map);
    } catch (...) {
      rpc_trace_guard->set_status(1);
      _post_client_pool->Remove(post_client_wrapper);
      LOG(error) << "Failed to read posts from post-storage-service";
      throw;
    }
  }
  _post_client_pool->Push(post_client_wrapper);
  span->Finish();
}

} // namespace social_network

#endif //SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_
