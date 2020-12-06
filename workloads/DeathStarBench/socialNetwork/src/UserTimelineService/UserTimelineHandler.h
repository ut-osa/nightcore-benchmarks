#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_

#include <iostream>
#include <string>
#include <future>

#include <mongoc.h>
#include <bson/bson.h>

#include "../../gen-cpp/UserTimelineService.h"
#include "../../gen-cpp/PostStorageService.h"
#include "../logger.h"
#include "../tracing.h"
#include "../ClientPool.h"
#include "../RedisClient.h"
#include "../ThriftClient.h"

namespace social_network {

class UserTimelineHandler : public UserTimelineServiceIf {
 public:
  UserTimelineHandler(
      ClientPool<RedisClient> *,
      mongoc_client_pool_t *,
      ClientPool<ThriftClient<PostStorageServiceClient>> *);
  ~UserTimelineHandler() override = default;

  void WriteUserTimeline(int64_t req_id, int64_t post_id, int64_t user_id,
      int64_t timestamp, const std::map<std::string, std::string> &carrier)
      override;

  void ReadUserTimeline(std::vector<Post> &, int64_t, int64_t, int, int,
                        const std::map<std::string, std::string> &) override ;

 private:
  ClientPool<RedisClient> *_redis_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
  ClientPool<ThriftClient<PostStorageServiceClient>> *_post_client_pool;
};

UserTimelineHandler::UserTimelineHandler(
    ClientPool<RedisClient> *redis_pool,
    mongoc_client_pool_t *mongodb_pool,
    ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool) {
  _redis_client_pool = redis_pool;
  _mongodb_client_pool = mongodb_pool;
  _post_client_pool = post_client_pool;
}

void UserTimelineHandler::WriteUserTimeline(
    int64_t req_id,
    int64_t post_id,
    int64_t user_id,
    int64_t timestamp,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "WriteUserTimeline",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
      _mongodb_client_pool);
  if (!mongodb_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to pop a client from MongoDB pool";
    throw se;
  }
  auto collection = mongoc_client_get_collection(
      mongodb_client, "user-timeline", "user-timeline");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection user-timeline from MongoDB";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }
  bson_t *query = bson_new();

  BSON_APPEND_INT64(query, "user_id", user_id);
  auto find_span = opentracing::Tracer::Global()->StartSpan(
      "MongoFindUser", {opentracing::ChildOf(&span->context())});
  mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
      collection, query, nullptr, nullptr);
  const bson_t *doc;
  bool found = mongoc_cursor_next(cursor, &doc);
  if (!found) {
    bson_t *new_doc = BCON_NEW(
        "user_id", BCON_INT64(user_id),
        "posts",
        "[", "{", "post_id", BCON_INT64(post_id),
        "timestamp", BCON_INT64(timestamp), "}", "]"
    );
    bson_error_t error;
    auto insert_span = opentracing::Tracer::Global()->StartSpan(
        "MongoInsert", {opentracing::ChildOf(&span->context())});
    bool inserted = mongoc_collection_insert_one(
        collection, new_doc, nullptr, nullptr, &error);
    insert_span->Finish();
    if (!inserted) {
      LOG(error) << "Failed to insert user timeline user " << user_id
                 << " to MongoDB: " << error.message;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = error.message;
      bson_destroy(new_doc);
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }
    bson_destroy(new_doc);
  } else {
    bson_t *update = BCON_NEW(
        "$push", "{",
            "posts", "{",
                "$each", "[", "{",
                    "post_id", BCON_INT64(post_id),
                    "timestamp", BCON_INT64(timestamp),
                "}", "]",
                "$position", BCON_INT32(0),
            "}",
        "}"
    );
    bson_error_t error;
    bson_t reply;
    auto update_span = opentracing::Tracer::Global()->StartSpan(
        "MongoInsert", {opentracing::ChildOf(&span->context())});
    bool updated = mongoc_collection_find_and_modify(
        collection, query, nullptr, update, nullptr, false, false,
        true, &reply, &error);
    update_span->Finish();
    if (!updated) {
      LOG(error) << "Failed to update user-timeline for user " << user_id
                 << " to MongoDB: " << error.message;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = error.message;
      bson_destroy(update);
      bson_destroy(query);
      bson_destroy(&reply);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }
    bson_destroy(update);
    bson_destroy(&reply);
  }
  bson_destroy(query);
  mongoc_cursor_destroy(cursor);
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

  auto redis_client_wrapper = _redis_client_pool->Pop();
  if (!redis_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_REDIS_ERROR;
    se.message = "Cannot connect to Redis server";
    throw se;
  }
  auto redis_client = redis_client_wrapper->GetClient();
  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "RedisUpdate", {opentracing::ChildOf(&span->context())});
  redis_client.AppendCommand("ZCARD %" PRId64, user_id);
  auto num_posts_reply = redis_client.GetReply();
  std::vector<std::string> options{"NX"};
  num_posts_reply->check_ok();
  if (num_posts_reply->as_integer()) {
    redis_client.AppendCommand("ZADD %" PRId64 " NX %" PRId64 " %" PRId64, user_id, timestamp, post_id);
    auto zadd_reply = redis_client.GetReply();
    zadd_reply->check_ok();
  }
  _redis_client_pool->Push(redis_client_wrapper);
  redis_span->Finish();
  span->Finish();

}
void UserTimelineHandler::ReadUserTimeline(
    std::vector<Post> &_return,
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
      "ReadUserTimeline",
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
  std::vector<int64_t> post_ids;
  auto post_ids_reply_array = post_ids_reply->as_array();
  for (auto &post_id_reply : post_ids_reply_array) {
    post_ids.emplace_back(std::stoul(post_id_reply->as_string()));
  }

  _redis_client_pool->Push(redis_client_wrapper);
  redis_span->Finish();

  int mongo_start = start + post_ids.size();
  std::multimap<std::string, std::string> redis_update_map;
  if (mongo_start < stop) {
    // Instead find post_ids from mongodb
    mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
        _mongodb_client_pool);
    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }
    auto collection = mongoc_client_get_collection(
        mongodb_client, "user-timeline", "user-timeline");
    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection user-timeline from MongoDB";
      throw se;
    }

    bson_t *query = BCON_NEW("user_id", BCON_INT64(user_id));
    bson_t *opts = BCON_NEW(
        "projection", "{",
            "posts", "{",
                "$slice", "[",
                    BCON_INT32(0), BCON_INT32(stop),
                "]",
            "}",
        "}");

    auto find_span = opentracing::Tracer::Global()->StartSpan(
        "MongoFindUserTimeline", { opentracing::ChildOf(&span->context()) });
    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
        collection, query, opts, nullptr);
    find_span->Finish();
    const bson_t *doc;
    bool found = mongoc_cursor_next(cursor, &doc);
    if (found) {
      bson_iter_t iter_0;
      bson_iter_t iter_1;
      bson_iter_t post_id_child;
      bson_iter_t timestamp_child;
      int idx = 0;
      bson_iter_init(&iter_0, doc);
      bson_iter_init(&iter_1, doc);
      while (bson_iter_find_descendant(&iter_0,
              ("posts." + std::to_string(idx) + ".post_id").c_str(),
              &post_id_child)
          && BSON_ITER_HOLDS_INT64(&post_id_child)
          && bson_iter_find_descendant(&iter_1,
              ("posts." + std::to_string(idx) + ".timestamp").c_str(),
              &timestamp_child)
          && BSON_ITER_HOLDS_INT64(&timestamp_child)) {
        auto curr_post_id = bson_iter_int64(&post_id_child);
        auto curr_timestamp = bson_iter_int64(&timestamp_child);
        if (idx >= mongo_start) {
          post_ids.emplace_back(curr_post_id);
        }
        redis_update_map.insert(std::make_pair(std::to_string(curr_timestamp),
            std::to_string(curr_post_id)));
        bson_iter_init(&iter_0, doc);
        bson_iter_init(&iter_1, doc);
        idx++;
      }
    }
    bson_destroy(opts);
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
  }

  // std::future<std::vector<Post>> post_future = std::async(
  //     std::launch::async, [&]() {
  {
        auto post_client_wrapper = _post_client_pool->Pop();
        if (!post_client_wrapper) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
          se.message = "Failed to connect to post-storage-service";
          throw se;
        }
        std::vector<Post> _return_posts;
          auto post_client = post_client_wrapper->GetClient();
          {
            auto rpc_trace_guard = _post_client_pool->StartRpcTrace("ReadPosts", post_client_wrapper);
            try {
              post_client->ReadPosts(
                  _return_posts, req_id, post_ids, writer_text_map);
            } catch (...) {
              rpc_trace_guard->set_status(1);
              _post_client_pool->Remove(post_client_wrapper);
              LOG(error) << "Failed to read posts from post-storage-service";
              throw;
            }
          }
          _post_client_pool->Push(post_client_wrapper);
          _return = std::move(_return_posts);
      //     return _return_posts;
      // });
  }

  if (!redis_update_map.empty()) {
    // Update Redis
    redis_client_wrapper = _redis_client_pool->Pop();
    if (!redis_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_REDIS_ERROR;
      se.message = "Cannot connect to Redis server";
      throw se;
    }
    redis_client = redis_client_wrapper->GetClient();
    auto redis_update_span = opentracing::Tracer::Global()->StartSpan(
        "RedisUpdate", {opentracing::ChildOf(&span->context())});
    redis_client.AppendCommand("DEL %" PRId64, user_id);
    std::stringstream cmd;
    cmd << "ZADD " << user_id << " NX";
    for (const auto& it : redis_update_map) {
      cmd << " " << it.first << " " << it.second;
    }
    redis_client.AppendCommand(cmd.str().c_str());
    auto del_reply = redis_client.GetReply();
    auto zadd_reply = redis_client.GetReply();
    del_reply->check_ok();
    zadd_reply->check_ok();
    _redis_client_pool->Push(redis_client_wrapper);
    redis_update_span->Finish();
  }

  // try {
  //   _return = post_future.get();
  // } catch (...) {
  //   LOG(error) << "Failed to get post from post-storage-service";
  //   throw;
  // }

  span->Finish();

}

} // namespace social_network


#endif //SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_
