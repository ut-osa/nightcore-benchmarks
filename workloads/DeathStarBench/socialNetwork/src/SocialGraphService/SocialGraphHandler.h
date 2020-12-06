#ifndef SOCIAL_NETWORK_MICROSERVICES_SOCIALGRAPHHANDLER_H
#define SOCIAL_NETWORK_MICROSERVICES_SOCIALGRAPHHANDLER_H

#include <iostream>
#include <string>
#include <chrono>
#include <thread>
#include <vector>
#include <future>

#include <mongoc.h>
#include <bson/bson.h>
#include <hiredis/hiredis.h>

#include "../../gen-cpp/SocialGraphService.h"
#include "../../gen-cpp/UserService.h"
#include "../ClientPool.h"
#include "../logger.h"
#include "../tracing.h"
#include "../RedisClient.h"
#include "../ThriftClient.h"

namespace social_network {

using std::chrono::milliseconds;
using std::chrono::duration_cast;
using std::chrono::system_clock;

class SocialGraphHandler : public SocialGraphServiceIf {
 public:
  SocialGraphHandler(
      mongoc_client_pool_t *,
      ClientPool<RedisClient> *,
      ClientPool<ThriftClient<UserServiceClient>> *);
  ~SocialGraphHandler() override = default;
  void GetFollowers(std::vector<int64_t> &, int64_t, int64_t,
                    const std::map<std::string, std::string> &) override;
  void GetFollowees(std::vector<int64_t> &, int64_t, int64_t,
                    const std::map<std::string, std::string> &) override;
  void Follow(int64_t, int64_t, int64_t,
              const std::map<std::string, std::string> &) override;
  void Unfollow(int64_t, int64_t, int64_t,
                const std::map<std::string, std::string> &) override;
  void FollowWithUsername(int64_t, const std::string &, const std::string &,
              const std::map<std::string, std::string> &) override;
  void UnfollowWithUsername(int64_t, const std::string &, const std::string &,
                const std::map<std::string, std::string> &) override;
  void InsertUser(int64_t, int64_t,
                  const std::map<std::string, std::string> &) override;


 private:
  mongoc_client_pool_t *_mongodb_client_pool;
  ClientPool<RedisClient> *_redis_client_pool;
  ClientPool<ThriftClient<UserServiceClient>> *_user_service_client_pool;
};

SocialGraphHandler::SocialGraphHandler(
    mongoc_client_pool_t *mongodb_client_pool,
    ClientPool<RedisClient> *redis_client_pool,
    ClientPool<ThriftClient<UserServiceClient>> *user_service_client_pool) {
  _mongodb_client_pool = mongodb_client_pool;
  _redis_client_pool = redis_client_pool;
  _user_service_client_pool = user_service_client_pool;
}

void SocialGraphHandler::Follow(
    int64_t req_id,
    int64_t user_id,
    int64_t followee_id,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "Follow",
      {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  int64_t timestamp = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()).count();

  // std::future<void> mongo_update_follower_future = std::async(
  //     std::launch::async, [&]() {
  {
        mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
            _mongodb_client_pool);
        if (!mongodb_client) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = "Failed to pop a client from MongoDB pool";
          throw se;
        }
        auto collection = mongoc_client_get_collection(
            mongodb_client, "social-graph", "social-graph");
        if (!collection) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = "Failed to create collection social_graph from MongoDB";
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          throw se;
        }

        // Update follower->followee edges
        const bson_t *doc;
        bson_t *search_not_exist = BCON_NEW(
            "$and", "[",
            "{", "user_id", BCON_INT64(user_id), "}", "{",
            "followees", "{", "$not", "{", "$elemMatch", "{",
            "user_id", BCON_INT64(followee_id), "}", "}", "}", "}", "]"
        );
        bson_t *update = BCON_NEW(
            "$push",
            "{",
            "followees",
            "{",
            "user_id",
            BCON_INT64(followee_id),
            "timestamp",
            BCON_INT64(timestamp),
            "}",
            "}"
        );
        bson_error_t error;
        bson_t reply;
        auto update_span = opentracing::Tracer::Global()->StartSpan(
            "MongoUpdateFollower", {opentracing::ChildOf(&span->context())});
        bool updated = mongoc_collection_find_and_modify(
            collection,
            search_not_exist,
            nullptr,
            update,
            nullptr,
            false,
            false,
            true,
            &reply,
            &error);
        if (!updated) {
          LOG(error) << "Failed to update social graph for user " << user_id
                     << " to MongoDB: " << error.message;
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = error.message;
          bson_destroy(&reply);
          bson_destroy(update);
          bson_destroy(search_not_exist);
          mongoc_collection_destroy(collection);
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          throw se;
        }
        update_span->Finish();
        bson_destroy(&reply);
        bson_destroy(update);
        bson_destroy(search_not_exist);
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      // });
  }

  // std::future<void> mongo_update_followee_future = std::async(
  //     std::launch::async, [&]() {
  {
        mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
            _mongodb_client_pool);
        if (!mongodb_client) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = "Failed to pop a client from MongoDB pool";
          throw se;
        }
        auto collection = mongoc_client_get_collection(
            mongodb_client, "social-graph", "social-graph");
        if (!collection) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = "Failed to create collection social_graph from MongoDB";
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          throw se;
        }

        // Update followee->follower edges
        bson_t *search_not_exist = BCON_NEW(
            "$and", "[", "{", "user_id", BCON_INT64(followee_id), "}", "{",
            "followers", "{", "$not", "{", "$elemMatch", "{",
            "user_id", BCON_INT64(user_id), "}", "}", "}", "}", "]"
        );
        bson_t *update = BCON_NEW(
            "$push", "{", "followers", "{", "user_id", BCON_INT64(user_id),
            "timestamp", BCON_INT64(timestamp), "}", "}"
        );
        bson_error_t error;
        auto update_span = opentracing::Tracer::Global()->StartSpan(
            "MongoUpdateFollowee", {opentracing::ChildOf(&span->context())});
        bson_t reply;
        bool updated = mongoc_collection_find_and_modify(
            collection, search_not_exist, nullptr, update, nullptr, false,
            false, true, &reply, &error);
        if (!updated) {
          LOG(error) << "Failed to update social graph for user "
                     << followee_id << " to MongoDB: " << error.message;
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = error.message;
          bson_destroy(update);
          bson_destroy(&reply);
          bson_destroy(search_not_exist);
          mongoc_collection_destroy(collection);
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          throw se;
        }
        update_span->Finish();
        bson_destroy(update);
        bson_destroy(&reply);
        bson_destroy(search_not_exist);
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      // });
  }

  // std::future<void> redis_update_future = std::async(
  //     std::launch::async, [&]() {
  {
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
        redis_client.AppendCommand("ZCARD %" PRId64 ":followees", user_id);
        redis_client.AppendCommand("ZCARD %" PRId64 ":followers", followee_id);
        auto num_followee_reply = redis_client.GetReply();
        auto num_follower_reply = redis_client.GetReply();
        num_followee_reply->check_ok();
        num_follower_reply->check_ok();

        if (num_followee_reply->as_integer()) {
          redis_client.AppendCommand("ZADD %" PRId64 ":followees NX %" PRId64 " %" PRId64,
                                     user_id, timestamp, followee_id);
        }
        if (num_follower_reply->as_integer()) {
          redis_client.AppendCommand("ZADD %" PRId64 ":followers NX %" PRId64 " %" PRId64,
                                     followee_id, timestamp, user_id);
        }
        if (num_followee_reply->as_integer()) {
          auto reply = redis_client.GetReply();
          reply->check_ok();
        }
        if (num_follower_reply->as_integer()) {
          auto reply = redis_client.GetReply();
          reply->check_ok();
        }
        _redis_client_pool->Push(redis_client_wrapper);
        redis_span->Finish();
      // });
  }

  // try {
  //   redis_update_future.get();
  //   mongo_update_follower_future.get();
  //   mongo_update_followee_future.get();
  // } catch (...) {
  //   throw;
  // }

  span->Finish();
}

void SocialGraphHandler::Unfollow(
    int64_t req_id,
    int64_t user_id,
    int64_t followee_id,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "Unfollow",
      {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  // std::future<void> mongo_update_follower_future = std::async(
  //     std::launch::async, [&]() {
  {
        mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
            _mongodb_client_pool);
        if (!mongodb_client) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = "Failed to pop a client from MongoDB pool";
          throw se;
        }
        auto collection = mongoc_client_get_collection(
            mongodb_client, "social-graph", "social-graph");
        if (!collection) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = "Failed to create collection social_graph from MongoDB";
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          throw se;
        }
        bson_t *query = bson_new();

        // Update follower->followee edges
        BSON_APPEND_INT64(query, "user_id", user_id);
        bson_t *update = BCON_NEW(
            "$pull", "{", "followees", "{",
            "user_id", BCON_INT64(followee_id), "}", "}"
        );
        bson_t reply;
        bson_error_t error;
        auto update_span = opentracing::Tracer::Global()->StartSpan(
            "MongoDeleteFollowee", {opentracing::ChildOf(&span->context())});
        bool updated = mongoc_collection_find_and_modify(
            collection, query, nullptr, update, nullptr, false, false,
            true, &reply, &error);
        if (!updated) {
          LOG(error) << "Failed to delete social graph for user " << user_id
                     << " to MongoDB: " << error.message;
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = error.message;
          bson_destroy(update);
          bson_destroy(query);
          bson_destroy(&reply);
          mongoc_collection_destroy(collection);
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          throw se;
        }
        update_span->Finish();
        bson_destroy(update);
        bson_destroy(query);
        bson_destroy(&reply);
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      // });
  }

  // std::future<void> mongo_update_followee_future = std::async(
  //     std::launch::async, [&]() {
  {
        mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
            _mongodb_client_pool);
        if (!mongodb_client) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = "Failed to pop a client from MongoDB pool";
          throw se;
        }
        auto collection = mongoc_client_get_collection(
            mongodb_client, "social-graph", "social-graph");
        if (!collection) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = "Failed to create collection social_graph from MongoDB";
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          throw se;
        }
        bson_t *query = bson_new();

        // Update followee->follower edges
        BSON_APPEND_INT64(query, "user_id", followee_id);
        bson_t *update = BCON_NEW(
            "$pull", "{", "followers", "{",
            "user_id", BCON_INT64(user_id), "}", "}"
        );
        bson_t reply;
        bson_error_t error;
        auto update_span = opentracing::Tracer::Global()->StartSpan(
            "MongoDeleteFollower", {opentracing::ChildOf(&span->context())});
        bool updated = mongoc_collection_find_and_modify(
            collection, query, nullptr, update, nullptr, false, false,
            true, &reply, &error);
        if (!updated) {
          LOG(error) << "Failed to delete social graph for user " << followee_id
                     << " to MongoDB: " << error.message;
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = error.message;
          bson_destroy(update);
          bson_destroy(query);
          bson_destroy(&reply);
          mongoc_collection_destroy(collection);
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          throw se;
        }
        update_span->Finish();
        bson_destroy(update);
        bson_destroy(query);
        bson_destroy(&reply);
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      // });
  }

  // std::future<void> redis_update_future = std::async(
  //     std::launch::async, [&]() {
  {
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
        redis_client.AppendCommand("ZCARD %" PRId64 ":followees", user_id);
        redis_client.AppendCommand("ZCARD %" PRId64 ":followers", followee_id);
        auto num_followee_reply = redis_client.GetReply();
        auto num_follower_reply = redis_client.GetReply();
        num_followee_reply->check_ok();
        num_follower_reply->check_ok();

        if (num_followee_reply->as_integer()) {
          redis_client.AppendCommand("ZREM %" PRId64 ":followees %" PRId64 " %" PRId64,
                                     user_id, followee_id);
        }
        if (num_follower_reply->as_integer()) {
          redis_client.AppendCommand("ZREM %" PRId64 ":followers NX %" PRId64 " %" PRId64,
                                     followee_id, user_id);
        }
        if (num_followee_reply->as_integer()) {
          auto reply = redis_client.GetReply();
          reply->check_ok();
        }
        if (num_follower_reply->as_integer()) {
          auto reply = redis_client.GetReply();
          reply->check_ok();
        }
        _redis_client_pool->Push(redis_client_wrapper);
        redis_span->Finish();
      // });
  }

  // try {
  //   redis_update_future.get();
  //   mongo_update_follower_future.get();
  //   mongo_update_followee_future.get();
  // } catch (...) {
  //   throw;
  // }

  span->Finish();

}

void SocialGraphHandler::GetFollowers(
    std::vector<int64_t> &_return, const int64_t req_id, const int64_t user_id,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "GetFollowers",
      {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto redis_client_wrapper = _redis_client_pool->Pop();
  if (!redis_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_REDIS_ERROR;
    se.message = "Cannot connect to Redis server";
    throw se;
  }
  auto redis_client = redis_client_wrapper->GetClient();

  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "RedisGet", {opentracing::ChildOf(&span->context())});
  redis_client.AppendCommand("ZCARD %" PRId64 ":followers", user_id);
  auto num_follower_reply = redis_client.GetReply();
  num_follower_reply->check_ok();

  if (num_follower_reply->as_integer()) {
    std::string key = std::to_string(user_id) + ":followers";
    redis_client.AppendCommand("ZRANGE %" PRId64 ":followers 0 -1 ", user_id);
    auto redis_followers_reply = redis_client.GetReply();
    redis_followers_reply->check_ok();
    redis_span->Finish();
    auto followers_str = redis_followers_reply->as_array();
    for (auto &item : followers_str) {
      _return.emplace_back(std::stoul(item->as_string()));
    }
    _redis_client_pool->Push(redis_client_wrapper);
    return;
  } else {
    redis_span->Finish();
    _redis_client_pool->Push(redis_client_wrapper);
    mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
        _mongodb_client_pool);
    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }
    auto collection = mongoc_client_get_collection(
        mongodb_client, "social-graph", "social-graph");
    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection social_graph from MongoDB";
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
    if (found) {
      bson_iter_t iter_0;
      bson_iter_t iter_1;
      bson_iter_t user_id_child;
      bson_iter_t timestamp_child;
      int index = 0;
      std::multimap<std::string, std::string> redis_zset;
      bson_iter_init(&iter_0, doc);
      bson_iter_init(&iter_1, doc);

      while (
          bson_iter_find_descendant(
              &iter_0,
              ("followers." + std::to_string(index) + ".user_id").c_str(),
              &user_id_child) &&
              BSON_ITER_HOLDS_INT64 (&user_id_child) &&
              bson_iter_find_descendant(
                  &iter_1,
                  ("followers." + std::to_string(index) + ".timestamp").c_str(),
                  &timestamp_child)
              && BSON_ITER_HOLDS_INT64 (&timestamp_child)) {

        auto iter_user_id = bson_iter_int64(&user_id_child);
        auto iter_timestamp = bson_iter_int64(&timestamp_child);
        _return.emplace_back(iter_user_id);
        redis_zset.emplace(std::pair<std::string, std::string>(
            std::to_string(iter_timestamp), std::to_string(iter_user_id)));
        bson_iter_init(&iter_0, doc);
        bson_iter_init(&iter_1, doc);
        index++;
      }
      find_span->Finish();
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

      redis_client_wrapper = _redis_client_pool->Pop();
      redis_client = redis_client_wrapper->GetClient();
      auto redis_insert_span = opentracing::Tracer::Global()->StartSpan(
          "RedisInsert", {opentracing::ChildOf(&span->context())});
      std::stringstream cmd;
      cmd << "ZADD " << user_id << ":followers NX";
      for (const auto& it : redis_zset) {
        cmd << " " << it.first << " " << it.second;
      }
      redis_client.AppendCommand(cmd.str().c_str());
      auto zadd_reply = redis_client.GetReply();
      zadd_reply->check_ok();
      redis_insert_span->Finish();
      _redis_client_pool->Push(redis_client_wrapper);
    } else {
      find_span->Finish();
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    }
  }
  span->Finish();
}

void SocialGraphHandler::GetFollowees(
    std::vector<int64_t> &_return, const int64_t req_id, const int64_t user_id,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "GetFollowees",
      {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto redis_client_wrapper = _redis_client_pool->Pop();
  if (!redis_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_REDIS_ERROR;
    se.message = "Cannot connect to Redis server";
    throw se;
  }
  auto redis_client = redis_client_wrapper->GetClient();

  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "RedisGet", {opentracing::ChildOf(&span->context())});
  redis_client.AppendCommand("ZCARD %" PRId64 ":followees", user_id);
  auto num_followees_reply = redis_client.GetReply();
  num_followees_reply->check_ok();

  if (num_followees_reply->as_integer()) {
    std::string key = std::to_string(user_id) + ":followees";
    redis_client.AppendCommand("ZRANGE %" PRId64 ":followees 0 -1", user_id);
    auto redis_followees_reply = redis_client.GetReply();
    redis_followees_reply->check_ok();
    auto followees_str = redis_followees_reply->as_array();
    for (auto &item : followees_str) {
      _return.emplace_back(std::stoul(item->as_string()));
    }
    _redis_client_pool->Push(redis_client_wrapper);
    return;
  } else {
    redis_span->Finish();
    mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
        _mongodb_client_pool);
    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }
    auto collection = mongoc_client_get_collection(
        mongodb_client, "social-graph", "social-graph");
    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection social_graph from MongoDB";
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
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
      se.message = "Cannot find user_id in MongoDB.";
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    } else {
      bson_iter_t iter_0;
      bson_iter_t iter_1;
      bson_iter_t user_id_child;
      bson_iter_t timestamp_child;
      int index = 0;
      std::multimap<std::string, std::string> redis_zset;
      bson_iter_init(&iter_0, doc);
      bson_iter_init(&iter_1, doc);

      while (
          bson_iter_find_descendant(
              &iter_0,
              ("followees." + std::to_string(index) + ".user_id").c_str(),
              &user_id_child) &&
              BSON_ITER_HOLDS_INT64 (&user_id_child) &&
              bson_iter_find_descendant(
                  &iter_1,
                  ("followees." + std::to_string(index) + ".timestamp").c_str(),
                  &timestamp_child)
              && BSON_ITER_HOLDS_INT64 (&timestamp_child)) {

        auto iter_user_id = bson_iter_int64(&user_id_child);
        auto iter_timestamp = bson_iter_int64(&timestamp_child);
        _return.emplace_back(iter_user_id);
        redis_zset.emplace(std::pair<std::string, std::string>(
            std::to_string(iter_timestamp), std::to_string(iter_user_id)));
        bson_iter_init(&iter_0, doc);
        bson_iter_init(&iter_1, doc);
        index++;
      }
      find_span->Finish();
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      redis_client_wrapper = _redis_client_pool->Pop();
      redis_client = redis_client_wrapper->GetClient();
      auto redis_insert_span = opentracing::Tracer::Global()->StartSpan(
          "RedisInsert", {opentracing::ChildOf(&span->context())});
      std::stringstream cmd;
      cmd << "ZADD " << user_id << ":followees NX";
      for (const auto& it : redis_zset) {
        cmd << " " << it.first << " " << it.second;
      }
      redis_client.AppendCommand(cmd.str().c_str());
      auto zadd_reply = redis_client.GetReply();
      zadd_reply->check_ok();
      redis_insert_span->Finish();
      _redis_client_pool->Push(redis_client_wrapper);
    }
  }
  span->Finish();
}

void SocialGraphHandler::InsertUser(
    int64_t req_id, int64_t user_id,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "InsertUser",
      {opentracing::ChildOf(parent_span->get())});
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
      mongodb_client, "social-graph", "social-graph");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection social_graph from MongoDB";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }

  bson_t *new_doc = BCON_NEW(
      "user_id", BCON_INT64(user_id),
      "followers", "[", "]",
      "followees", "[", "]"
  );
  bson_error_t error;
  auto insert_span = opentracing::Tracer::Global()->StartSpan(
      "MongoInsertUser", {opentracing::ChildOf(&span->context())});
  bool inserted = mongoc_collection_insert_one(
      collection, new_doc, nullptr, nullptr, &error);
  insert_span->Finish();
  if (!inserted) {
    LOG(error) << "Failed to insert social graph for user "
               << user_id << " to MongoDB: " << error.message;
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = error.message;
    bson_destroy(new_doc);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }
  bson_destroy(new_doc);
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
  span->Finish();
}

void SocialGraphHandler::FollowWithUsername(
    int64_t req_id,
    const std::string &user_name,
    const std::string &followee_name,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "FollowWithUsername",
      {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  int64_t user_id;
  int64_t followee_id;

  // std::future<int64_t> user_id_future = std::async(
  //     std::launch::async,[&]() {
  {
        auto user_client_wrapper = _user_service_client_pool->Pop();
        if (!user_client_wrapper) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
          se.message = "Failed to connect to social-graph-service";
          throw se;
        }
        auto user_client = user_client_wrapper->GetClient();
        int64_t _return;
        {
          auto rpc_trace_guard = _user_service_client_pool->StartRpcTrace("GetUserId", user_client_wrapper);
          try {
            _return = user_client->GetUserId(req_id, user_name, writer_text_map);
          } catch (...) {
            rpc_trace_guard->set_status(1);
            _user_service_client_pool->Remove(user_client_wrapper);
            LOG(error) << "Failed to get user_id from user-service";
            throw;
          }
        }
        _user_service_client_pool->Push(user_client_wrapper);
        // return _return;
        user_id = _return;
      // });
  }

  // std::future<int64_t> followee_id_future = std::async(
  //     std::launch::async,[&]() {
  {
        auto user_client_wrapper = _user_service_client_pool->Pop();
        if (!user_client_wrapper) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
          se.message = "Failed to connect to social-graph-service";
          throw se;
        }
        auto user_client = user_client_wrapper->GetClient();
        int64_t _return;
        {
          auto rpc_trace_guard = _user_service_client_pool->StartRpcTrace("GetUserId", user_client_wrapper);
          try {
            _return = user_client->GetUserId(req_id, followee_name, writer_text_map);
          } catch (...) {
            rpc_trace_guard->set_status(1);
            _user_service_client_pool->Remove(user_client_wrapper);
            LOG(error) << "Failed to get user_id from user-service";
            throw;
          }
        }
        _user_service_client_pool->Push(user_client_wrapper);
        // return _return;
        followee_id = _return;
      // });
  }

  // int64_t user_id;
  // int64_t followee_id;
  // try {
  //   user_id = user_id_future.get();
  //   followee_id = followee_id_future.get();
  // } catch (...) {
  //   throw;
  // }

  if (user_id >= 0 && followee_id >= 0) {
    try {
      Follow(req_id, user_id, followee_id, writer_text_map);
    } catch (...) {
      throw;
    }
  }
  span->Finish();
}

void SocialGraphHandler::UnfollowWithUsername(
    int64_t req_id,
    const std::string &user_name,
    const std::string &followee_name,
    const std::map<std::string, std::string> &carrier) {
// Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UnfollowWithUsername",
      {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  int64_t user_id;
  int64_t followee_id;

  // std::future<int64_t> user_id_future = std::async(
  //     std::launch::async,[&]() {
  {
        auto user_client_wrapper = _user_service_client_pool->Pop();
        if (!user_client_wrapper) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
          se.message = "Failed to connect to social-graph-service";
          throw se;
        }
        auto user_client = user_client_wrapper->GetClient();
        int64_t _return;
        {
          auto rpc_trace_guard = _user_service_client_pool->StartRpcTrace("GetUserId", user_client_wrapper);
          try {
            _return = user_client->GetUserId(req_id, user_name, writer_text_map);
          } catch (...) {
            rpc_trace_guard->set_status(1);
            _user_service_client_pool->Remove(user_client_wrapper);
            LOG(error) << "Failed to get user_id from user-service";
            throw;
          }
        }
        _user_service_client_pool->Push(user_client_wrapper);
        user_id = _return;
      //   return _return;
      // });
  }

  // std::future<int64_t> followee_id_future = std::async(
  //     std::launch::async,[&]() {
  {
        auto user_client_wrapper = _user_service_client_pool->Pop();
        if (!user_client_wrapper) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
          se.message = "Failed to connect to social-graph-service";
          throw se;
        }
        auto user_client = user_client_wrapper->GetClient();
        int64_t _return;
        {
          auto rpc_trace_guard = _user_service_client_pool->StartRpcTrace("GetUserId", user_client_wrapper);
          try {
            _return = user_client->GetUserId(req_id, followee_name, writer_text_map);
          } catch (...) {
            rpc_trace_guard->set_status(1);
            _user_service_client_pool->Remove(user_client_wrapper);
            LOG(error) << "Failed to get user_id from user-service";
            throw;
          }
        }
        _user_service_client_pool->Push(user_client_wrapper);
        followee_id = _return;
      //   return _return;
      // });
  }

  // int64_t user_id;
  // int64_t followee_id;
  // try {
  //   user_id = user_id_future.get();
  //   followee_id = followee_id_future.get();
  // } catch (...) {
  //   throw;
  // }

  if (user_id >= 0 && followee_id >= 0) {
    try {
      Unfollow(req_id, user_id, followee_id, writer_text_map);
    } catch (...) {
      throw;
    }
  }
  span->Finish();
}

} // namespace social_network

#endif //SOCIAL_NETWORK_MICROSERVICES_SOCIALGRAPHHANDLER_H
