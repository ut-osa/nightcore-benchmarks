#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_COMPOSEPOSTSERVICE_COMPOSEPOSTHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_COMPOSEPOSTSERVICE_COMPOSEPOSTHANDLER_H_

#include <iostream>
#include <string>
#include <vector>
#include <chrono>

#include <hiredis/hiredis.h>
#include <nlohmann/json.hpp>

#include "../../gen-cpp/ComposePostService.h"
#include "../../gen-cpp/PostStorageService.h"
#include "../../gen-cpp/UserTimelineService.h"
#include "../ClientPool.h"
#include "../logger.h"
#include "../tracing.h"
#include "../RedisClient.h"
#include "../ThriftClient.h"
#include "RabbitmqClient.h"

#define NUM_COMPONENTS 6
#define REDIS_EXPIRE_TIME 10

namespace social_network {
using json = nlohmann::json;
using std::chrono::milliseconds;
using std::chrono::duration_cast;
using std::chrono::system_clock;

class ComposePostHandler : public ComposePostServiceIf {
 public:
  ComposePostHandler(
      ClientPool<RedisClient> *,
      ClientPool<ThriftClient<PostStorageServiceClient>> *,
      ClientPool<ThriftClient<UserTimelineServiceClient>> *,
      ClientPool<RabbitmqClient> *rabbitmq_client_pool);
  ~ComposePostHandler() override = default;

  void UploadText(int64_t req_id, const std::string& text,
      const std::map<std::string, std::string> & carrier) override;

  void UploadMedia(int64_t req_id, const std::vector<Media>& media,
      const std::map<std::string, std::string> & carrier) override;

  void UploadUniqueId(int64_t req_id, const int64_t post_id,
      const PostType::type post_type,
      const std::map<std::string, std::string> & carrier) override;

  void UploadCreator(int64_t req_id, const Creator& creator,
      const std::map<std::string, std::string> & carrier) override;

  void UploadUrls(int64_t req_id, const std::vector<Url> & urls,
      const std::map<std::string, std::string> & carrier) override;

  void UploadUserMentions(const int64_t req_id,
      const std::vector<UserMention> & user_mentions,
      const std::map<std::string, std::string> & carrier) override;

 private:
  ClientPool<RedisClient> *_redis_client_pool;
  ClientPool<ThriftClient<PostStorageServiceClient>>
      *_post_storage_client_pool;
  ClientPool<ThriftClient<UserTimelineServiceClient>>
      *_user_timeline_client_pool;
  ClientPool<RabbitmqClient> *_rabbitmq_client_pool;
  std::exception_ptr _rabbitmq_teptr;
  std::exception_ptr _post_storage_teptr;
  std::exception_ptr _user_timeline_teptr;

  void _ComposeAndUpload(int64_t req_id,
      const std::map<std::string, std::string> & carrier);

  void _UploadUserTimelineHelper(int64_t req_id, int64_t post_id,
      int64_t user_id, int64_t timestamp,
      const std::map<std::string, std::string> & carrier);

  void _UploadPostHelper(int64_t req_id, const Post &post,
      const std::map<std::string, std::string> &carrier);

  void _UploadHomeTimelineHelper(int64_t req_id, int64_t post_id,
      int64_t user_id, int64_t timestamp,
      const std::vector<int64_t> &user_mentions_id,
      const std::map<std::string, std::string> &carrier);

};

ComposePostHandler::ComposePostHandler(
    ClientPool<social_network::RedisClient> * redis_client_pool,
    ClientPool<social_network::ThriftClient<
        PostStorageServiceClient>> *post_storage_client_pool,
    ClientPool<social_network::ThriftClient<
        UserTimelineServiceClient>> *user_timeline_client_pool,
    ClientPool<RabbitmqClient> *rabbitmq_client_pool) {
  _redis_client_pool = redis_client_pool;
  _post_storage_client_pool = post_storage_client_pool;
  _user_timeline_client_pool = user_timeline_client_pool;
  _rabbitmq_client_pool = rabbitmq_client_pool;
  _rabbitmq_teptr = nullptr;
  _post_storage_teptr = nullptr;
  _user_timeline_teptr = nullptr;
}

void ComposePostHandler::UploadCreator(
    int64_t req_id,
    const Creator &creator,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadCreator",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::string creator_str = "{\"user_id\": " + std::to_string(creator.user_id)
      + ", \"username\": \"" + creator.username + "\"}";

  auto redis_client_wrapper = _redis_client_pool->Pop();
  if (!redis_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_REDIS_ERROR;
    se.message = "Cannot connect to Redis server";
    throw se;
  }
  auto redis_client = redis_client_wrapper->GetClient();
  auto add_span = opentracing::Tracer::Global()->StartSpan(
      "RedisHashSet", {opentracing::ChildOf(&span->context())});
  redis_client.AppendCommand("HSET %" PRId64 " creator %s", req_id, creator_str.c_str());
  redis_client.AppendCommand("HINCRBY %" PRId64 " num_components 1", req_id);
  redis_client.AppendCommand("EXPIRE %" PRId64 " %d", req_id, REDIS_EXPIRE_TIME);

  auto hset_reply = redis_client.GetReply();
  auto num_components_reply = redis_client.GetReply();
  auto expire_reply = redis_client.GetReply();
  _redis_client_pool->Push(redis_client_wrapper);

  hset_reply->check_ok();
  expire_reply->check_ok();

  if (num_components_reply->as_integer() == NUM_COMPONENTS) {
    _ComposeAndUpload(req_id, writer_text_map);
  }

  span->Finish();

}

void ComposePostHandler::UploadText(
    int64_t req_id,
    const std::string &text,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadText",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto redis_client_wrapper = _redis_client_pool->Pop();
  if (!redis_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_REDIS_ERROR;
    se.message = "Cannot connect to Redis server";
    throw se;
  }
  auto redis_client = redis_client_wrapper->GetClient();
  auto add_span = opentracing::Tracer::Global()->StartSpan(
      "RedisHashSet", {opentracing::ChildOf(&span->context())});
  redis_client.AppendCommand("HSET %" PRId64 " text %s", req_id, text.c_str());
  redis_client.AppendCommand("HINCRBY %" PRId64 " num_components 1", req_id);
  redis_client.AppendCommand("EXPIRE %" PRId64 " %d", req_id, REDIS_EXPIRE_TIME);

  auto hset_reply = redis_client.GetReply();
  auto num_components_reply = redis_client.GetReply();
  auto expire_reply = redis_client.GetReply();
  _redis_client_pool->Push(redis_client_wrapper);

  hset_reply->check_ok();
  expire_reply->check_ok();

  if (num_components_reply->as_integer() == NUM_COMPONENTS) {
    _ComposeAndUpload(req_id, writer_text_map);
  }

  span->Finish();

}

void ComposePostHandler::UploadMedia(
    int64_t req_id,
    const std::vector<Media> &media,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadMedia",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::string media_str = "[";
  if (!media.empty()) {
    for (auto &item : media) {
      media_str += "{\"media_id\": " + std::to_string(item.media_id) +
          ", \"media_type\": \"" + item.media_type + "\"},";
    }
    media_str.pop_back();
  }
  media_str += "]";

  auto redis_client_wrapper = _redis_client_pool->Pop();
  if (!redis_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_REDIS_ERROR;
    se.message = "Cannot connect to Redis server";
    throw se;
  }
  auto redis_client = redis_client_wrapper->GetClient();
  auto add_span = opentracing::Tracer::Global()->StartSpan(
      "RedisHashSet", {opentracing::ChildOf(&span->context())});
  redis_client.AppendCommand("HSET %" PRId64 " media %s", req_id, media_str.c_str());
  redis_client.AppendCommand("HINCRBY %" PRId64 " num_components 1", req_id);
  redis_client.AppendCommand("EXPIRE %" PRId64 " %d", req_id, REDIS_EXPIRE_TIME);

  auto hset_reply = redis_client.GetReply();
  auto num_components_reply = redis_client.GetReply();
  auto expire_reply = redis_client.GetReply();
  _redis_client_pool->Push(redis_client_wrapper);

  hset_reply->check_ok();
  expire_reply->check_ok();

  if (num_components_reply->as_integer() == NUM_COMPONENTS) {
    _ComposeAndUpload(req_id, writer_text_map);
  }

  span->Finish();

}

void ComposePostHandler::UploadUniqueId(
    int64_t req_id,
    const int64_t post_id,
    const PostType::type post_type,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadUniqueId",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto redis_client_wrapper = _redis_client_pool->Pop();
  if (!redis_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_REDIS_ERROR;
    se.message = "Cannot connect to Redis server";
    throw se;
  }
  auto redis_client = redis_client_wrapper->GetClient();
  auto add_span = opentracing::Tracer::Global()->StartSpan(
      "RedisHashSet", {opentracing::ChildOf(&span->context())});
  redis_client.AppendCommand("HSET %" PRId64 " post_id %" PRId64, req_id, post_id);
  redis_client.AppendCommand("HSET %" PRId64 " post_type %s", req_id, std::to_string(post_type).c_str());
  redis_client.AppendCommand("HINCRBY %" PRId64 " num_components 1", req_id);
  redis_client.AppendCommand("EXPIRE %" PRId64 " %d", req_id, REDIS_EXPIRE_TIME);

  auto hset_reply_0 = redis_client.GetReply();
  auto hset_reply_1 = redis_client.GetReply();
  auto num_components_reply = redis_client.GetReply();
  auto expire_reply = redis_client.GetReply();
  _redis_client_pool->Push(redis_client_wrapper);

  hset_reply_0->check_ok();
  hset_reply_1->check_ok();
  expire_reply->check_ok();

  if (num_components_reply->as_integer() == NUM_COMPONENTS) {
    _ComposeAndUpload(req_id, writer_text_map);
  }

  span->Finish();

}

void ComposePostHandler::UploadUrls(
    int64_t req_id,
    const std::vector<Url> &urls,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadUrls",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::string urls_str = "[";
  if (!urls.empty()) {
    for (auto &item : urls) {
      urls_str += "{\"shortened_url\": \"" + item.shortened_url +
          "\", \"expanded_url\": \"" + item.expanded_url + "\"},";
    }
    urls_str.pop_back();
  }
  urls_str += "]";

  auto redis_client_wrapper = _redis_client_pool->Pop();
  if (!redis_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_REDIS_ERROR;
    se.message = "Cannot connect to Redis server";
    throw se;
  }
  auto redis_client = redis_client_wrapper->GetClient();
  auto add_span = opentracing::Tracer::Global()->StartSpan(
      "RedisHashSet", {opentracing::ChildOf(&span->context())});
  redis_client.AppendCommand("HSET %" PRId64 " urls %s", req_id, urls_str.c_str());
  redis_client.AppendCommand("HINCRBY %" PRId64 " num_components 1", req_id);
  redis_client.AppendCommand("EXPIRE %" PRId64 " %d", req_id, REDIS_EXPIRE_TIME);

  auto hset_reply = redis_client.GetReply();
  auto num_components_reply = redis_client.GetReply();
  auto expire_reply = redis_client.GetReply();
  _redis_client_pool->Push(redis_client_wrapper);

  hset_reply->check_ok();
  expire_reply->check_ok();

  if (num_components_reply->as_integer() == NUM_COMPONENTS) {
    _ComposeAndUpload(req_id, writer_text_map);
  }

  span->Finish();

}

void ComposePostHandler::UploadUserMentions(
    const int64_t req_id,
    const std::vector<UserMention> &user_mentions,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadUserMentions",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::string user_mentions_str = "[";
  if (!user_mentions.empty()) {
    for (auto &item : user_mentions) {
      user_mentions_str += "{\"user_id\": " + std::to_string(item.user_id) +
          ", \"username\": \"" + item.username + "\"},";
    }
    user_mentions_str.pop_back();
  }
  user_mentions_str += "]";

  auto redis_client_wrapper = _redis_client_pool->Pop();
  if (!redis_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_REDIS_ERROR;
    se.message = "Cannot connect to Redis server";
    throw se;
  }
  auto redis_client = redis_client_wrapper->GetClient();
  auto add_span = opentracing::Tracer::Global()->StartSpan(
      "RedisHashSet", {opentracing::ChildOf(&span->context())});
  redis_client.AppendCommand("HSET %" PRId64 " user_mentions %s", req_id, user_mentions_str.c_str());
  redis_client.AppendCommand("HINCRBY %" PRId64 " num_components 1", req_id);
  redis_client.AppendCommand("EXPIRE %" PRId64 " %d", req_id, REDIS_EXPIRE_TIME);

  auto hset_reply = redis_client.GetReply();
  auto num_components_reply = redis_client.GetReply();
  auto expire_reply = redis_client.GetReply();
  _redis_client_pool->Push(redis_client_wrapper);

  hset_reply->check_ok();
  expire_reply->check_ok();

  if (num_components_reply->as_integer() == NUM_COMPONENTS) {
    _ComposeAndUpload(req_id, writer_text_map);
  }


  span->Finish();

}

void ComposePostHandler::_ComposeAndUpload(
    int64_t req_id,
    const std::map<std::string, std::string> &carrier) {

  auto redis_client_wrapper = _redis_client_pool->Pop();
  if (!redis_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_REDIS_ERROR;
    se.message = "Cannot connect to Redis server";
    throw se;
  }
  auto redis_client = redis_client_wrapper->GetClient();
  redis_client.AppendCommand("HGET %" PRId64 " text", req_id);
  redis_client.AppendCommand("HGET %" PRId64 " creator", req_id);
  redis_client.AppendCommand("HGET %" PRId64 " media", req_id);
  redis_client.AppendCommand("HGET %" PRId64 " post_id", req_id);
  redis_client.AppendCommand("HGET %" PRId64 " urls", req_id);
  redis_client.AppendCommand("HGET %" PRId64 " user_mentions", req_id);
  redis_client.AppendCommand("HGET %" PRId64 " post_type", req_id);

  auto text_reply = redis_client.GetReply();
  auto creator_reply = redis_client.GetReply();
  auto media_reply = redis_client.GetReply();
  auto post_id_reply = redis_client.GetReply();
  auto urls_reply = redis_client.GetReply();
  auto user_mentions_reply = redis_client.GetReply();
  auto post_type_reply = redis_client.GetReply();

  _redis_client_pool->Push(redis_client_wrapper);

  // Compose the post
  Post post;
  post.req_id = req_id;
  post.text = text_reply->as_string();
  post.post_id = std::stoul(post_id_reply->as_string());
  post.timestamp = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()).count();
  post.post_type = static_cast<PostType::type>(stoi(post_type_reply->as_string()));

  LOG(debug) << creator_reply->as_string();

  json creator_json = json::parse(creator_reply->as_string());
  post.creator.user_id = creator_json["user_id"];
  post.creator.username = creator_json["username"];

  LOG(debug) << user_mentions_reply->as_string();

  std::vector<int64_t> user_mentions_id;

  json user_mentions_json = json::parse(user_mentions_reply->as_string());
  for (auto &item : user_mentions_json) {
    UserMention user_mention;
    user_mention.user_id = item["user_id"];
    user_mention.username = item["username"];
    post.user_mentions.emplace_back(user_mention);
    user_mentions_id.emplace_back(user_mention.user_id);
  }

  json media_json = json::parse(media_reply->as_string());
  for (auto &item : media_json) {
    Media media;
    media.media_id = item["media_id"];
    media.media_type = item["media_type"];
    post.media.emplace_back(media);
  }

  json urls_json = json::parse(urls_reply->as_string());
  for (auto &item : urls_json) {
    Url url;
    url.shortened_url = item["shortened_url"];
    url.expanded_url = item["expanded_url"];
    post.urls.emplace_back(url);
  }

  _user_timeline_teptr = nullptr;
  _rabbitmq_teptr = nullptr;
  _post_storage_teptr = nullptr;

  _UploadPostHelper(req_id, std::ref(post), std::ref(carrier));
  _UploadUserTimelineHelper(req_id, post.post_id, post.creator.user_id, post.timestamp, std::ref(carrier));
  _UploadHomeTimelineHelper(req_id, post.post_id, post.creator.user_id, post.timestamp,
                            std::ref(user_mentions_id), std::ref(carrier));

  // Upload the post
  // std::thread upload_post_worker(&ComposePostHandler::_UploadPostHelper,
  //                                  this, req_id, std::ref(post), std::ref(carrier));

  // std::thread upload_user_timeline_worker(
  //     &ComposePostHandler::_UploadUserTimelineHelper, this, req_id,
  //     post.post_id, post.creator.user_id, post.timestamp, std::ref(carrier));

  // std::thread upload_home_timeline_worker(
  //     &ComposePostHandler::_UploadHomeTimelineHelper, this, req_id,
  //     post.post_id, post.creator.user_id, post.timestamp,
  //     std::ref(user_mentions_id), std::ref(carrier));

  // upload_post_worker.join();
  // upload_user_timeline_worker.join();
  // upload_home_timeline_worker.join();

  if (_user_timeline_teptr) {
    try{
      std::rethrow_exception(_user_timeline_teptr);
    }
    catch(const std::exception &ex)
    {
      LOG(error) << "Thread exited with exception: " << ex.what();
    }
  }
  if (_rabbitmq_teptr) {
    try{
      std::rethrow_exception(_rabbitmq_teptr);
    }
    catch(const std::exception &ex)
    {
      LOG(error) << "Thread exited with exception: " << ex.what();
    }
  }
  if (_post_storage_teptr) {
    try{
      std::rethrow_exception(_post_storage_teptr);
    }
    catch(const std::exception &ex)
    {
      LOG(error) << "Thread exited with exception: " << ex.what();
    }
  }
}

void ComposePostHandler::_UploadPostHelper(
    int64_t req_id,
    const Post &post,
    const std::map<std::string, std::string> &carrier) {
  try{
    auto post_storage_client_wrapper = _post_storage_client_pool->Pop();
    if (!post_storage_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to post-storage-service";
      throw se;
    }
    auto post_storage_client = post_storage_client_wrapper->GetClient();
    {
      auto rpc_trace_guard = _post_storage_client_pool->StartRpcTrace("StorePost", post_storage_client_wrapper);
      try {
        post_storage_client->StorePost(req_id, post, carrier);
      } catch (...) {
        rpc_trace_guard->set_status(1);
        _post_storage_client_pool->Remove(post_storage_client_wrapper);
        LOG(error) << "Failed to store post to post-storage-service";
        throw;
      }
    }
    _post_storage_client_pool->Push(post_storage_client_wrapper);
  } catch (...) {
    LOG(error) << "Failed to connect to post-storage-service";
    _post_storage_teptr = std::current_exception();
  }
}

void ComposePostHandler::_UploadUserTimelineHelper(
    int64_t req_id,
    int64_t post_id,
    int64_t user_id,
    int64_t timestamp,
    const std::map<std::string, std::string> &carrier) {
  try{
    auto user_timeline_client_wrapper = _user_timeline_client_pool->Pop();
    if (!user_timeline_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connect to user-timeline-service";
      throw se;
    }
    auto user_timeline_client = user_timeline_client_wrapper->GetClient();
    {
      auto rpc_trace_guard = _user_timeline_client_pool->StartRpcTrace("WriteUserTimeline", user_timeline_client_wrapper);
      try {
        user_timeline_client->WriteUserTimeline(req_id, post_id, user_id,
                                                timestamp, carrier);
      } catch (...) {
        rpc_trace_guard->set_status(1);
        _user_timeline_client_pool->Remove(user_timeline_client_wrapper);
        throw;
      }
    }
    _user_timeline_client_pool->Push(user_timeline_client_wrapper);
  } catch (...) {
    LOG(error) << "Failed to write user-timeline to user-timeline-service";
    _user_timeline_teptr = std::current_exception();
  }
}

void ComposePostHandler::_UploadHomeTimelineHelper(
    int64_t req_id,
    int64_t post_id,
    int64_t user_id,
    int64_t timestamp,
    const std::vector<int64_t> &user_mentions_id,
    const std::map<std::string, std::string> &carrier) {
  try {
    std::string user_mentions_id_str = "[";
    for (auto &i : user_mentions_id){
      user_mentions_id_str += std::to_string(i) + ", ";
    }
    user_mentions_id_str = user_mentions_id_str.substr(0,
        user_mentions_id_str.length() - 2);
    user_mentions_id_str += "]";
    std::string carrier_str = "{";
    for (auto &item : carrier) {
      carrier_str += "\"" + item.first + "\" : \"" + item.second + "\", ";
    }
    carrier_str = carrier_str.substr(0, carrier_str.length() - 2);
    carrier_str += "}";

    std::string msg_str = "{ \"req_id\": " + std::to_string(req_id) +
        ", \"post_id\": " + std::to_string(post_id) +
        ", \"user_id\": " + std::to_string(user_id) +
        ", \"timestamp\": " + std::to_string(timestamp) +
        ", \"user_mentions_id\": " + user_mentions_id_str +
        ", \"carrier\": " + carrier_str + "}";

    auto rabbitmq_client_wrapper = _rabbitmq_client_pool->Pop();
    if (!rabbitmq_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_RABBITMQ_CONN_ERROR;
      se.message = "Failed to connect to home-timeline-rabbitmq";
      throw se;
    }
    auto rabbitmq_channel = rabbitmq_client_wrapper->GetChannel();
    {
      auto rpc_trace_guard = _rabbitmq_client_pool->StartRpcTrace("Publish", rabbitmq_client_wrapper);
      auto msg = AmqpClient::BasicMessage::Create(msg_str);
      rabbitmq_channel->BasicPublish("", "write-home-timeline", msg);
      _rabbitmq_client_pool->Push(rabbitmq_client_wrapper);
    }
  } catch (...) {
    LOG(error) << "Failed to connect to home-timeline-rabbitmq";
    _rabbitmq_teptr = std::current_exception();
  }
}



} // namespace social_network

#endif //SOCIAL_NETWORK_MICROSERVICES_SRC_COMPOSEPOSTSERVICE_COMPOSEPOSTHANDLER_H_
