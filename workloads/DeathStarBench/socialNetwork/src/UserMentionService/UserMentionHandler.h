#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_USERMENTIONSERVICE_USERMENTIONHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_USERMENTIONSERVICE_USERMENTIONHANDLER_H_

#include <mongoc.h>
#include <bson.h>

#include "../../gen-cpp/UserMentionService.h"
#include "../../gen-cpp/ComposePostService.h"
#include "../../gen-cpp/social_network_types.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"
#include "../utils.h"

namespace social_network {

uint64_t time_us() {
    struct timeval t;
    gettimeofday(&t, NULL);
    return (t.tv_sec * 1000000) + t.tv_usec;
}

class UserMentionHandler : public UserMentionServiceIf {
 public:
  UserMentionHandler(ClientPool<MCClient> *,
                     mongoc_client_pool_t *,
                     ClientPool<ThriftClient<ComposePostServiceClient>> *);
  ~UserMentionHandler() override = default;

  void UploadUserMentions(int64_t, const std::vector<std::string> &,
      const std::map<std::string, std::string> &) override ;

 private:
  ClientPool<MCClient> *_mc_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
  ClientPool<ThriftClient<ComposePostServiceClient>> *_compose_client_pool;
};

UserMentionHandler::UserMentionHandler(
    ClientPool<MCClient> *mc_client_pool,
    mongoc_client_pool_t *mongodb_client_pool,
    ClientPool<ThriftClient<ComposePostServiceClient>> *compose_client_pool) {
  _mc_client_pool = mc_client_pool;
  _mongodb_client_pool = mongodb_client_pool;
  _compose_client_pool = compose_client_pool;
}

void UserMentionHandler::UploadUserMentions(
    int64_t req_id,
    const std::vector<std::string> &usernames,
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

  uint64_t start_time;
  uint64_t elapsed_time;

  std::vector<UserMention> user_mentions;
  if (!usernames.empty()) {
    std::map<std::string, bool> usernames_not_cached;

    for (auto &username : usernames) {
      usernames_not_cached.emplace(std::make_pair(username, false));
    }

    // Find in Memcached
    auto mc_client = _mc_client_pool->Pop();
    if (!mc_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message = "Failed to pop a client from memcached pool";
      throw se;
    }

    bool found;
    std::string return_value;
    uint32_t flags;

    for (int i = 0; i < usernames.size(); i++) {
      std::string key_str = usernames[i] + ":user_id";
      start_time = time_us();
      bool success = mc_client->Get(key_str, &found, &return_value, &flags);
      elapsed_time = time_us() - start_time;
      if (elapsed_time > 10000) { // 10ms
        LOG(info) << "Single memcached_get uses " << elapsed_time << "us";
      }
      if (!success) {
        _mc_client_pool->Push(mc_client);
        LOG(error) << "Cannot get components of request " << req_id;
        ServiceException se;
        se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
        se.message =  "Cannot get usernames of request " + std::to_string(req_id);
        throw se;
      }
      if (!found) continue;
      UserMention new_user_mention;
      new_user_mention.username = usernames[i];
      new_user_mention.user_id = std::stoul(return_value);
      user_mentions.emplace_back(new_user_mention);
      usernames_not_cached.erase(usernames[i]);
    }

    _mc_client_pool->Push(mc_client);

    // Find the rest in MongoDB
    if (!usernames_not_cached.empty()) {
      start_time = time_us();

      mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
          _mongodb_client_pool);
      if (!mongodb_client) {
        ServiceException se;
        se.errorCode = ErrorCode::SE_MONGODB_ERROR;
        se.message = "Failed to pop a client from MongoDB pool";
        throw se;
      }

      auto collection = mongoc_client_get_collection(
          mongodb_client, "user", "user");
      if (!collection) {
        ServiceException se;
        se.errorCode = ErrorCode::SE_MONGODB_ERROR;
        se.message = "Failed to create collection user from DB user";
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        throw se;
      }

      bson_t *query = bson_new();
      bson_t query_child_0;
      bson_t query_username_list;
      const char *key;
      int idx = 0;
      char buf[16];

      BSON_APPEND_DOCUMENT_BEGIN(query, "username", &query_child_0);
      BSON_APPEND_ARRAY_BEGIN(&query_child_0, "$in", &query_username_list);
      for (auto &item : usernames_not_cached) {
        bson_uint32_to_string(idx, &key, buf, sizeof buf);
        BSON_APPEND_UTF8(&query_username_list, key, item.first.c_str());
        idx++;
      }
      bson_append_array_end(&query_child_0, &query_username_list);
      bson_append_document_end(query, &query_child_0);

      mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
          collection, query, nullptr, nullptr);
      const bson_t *doc;

      while (mongoc_cursor_next(cursor, &doc)) {
        bson_iter_t iter;
        UserMention new_user_mention;
        if (bson_iter_init_find(&iter, doc, "user_id")) {
          new_user_mention.user_id = bson_iter_value(&iter)->value.v_int64;
        } else {
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = "Attribute of MongoDB item is not complete";
          bson_destroy(query);
          mongoc_cursor_destroy(cursor);
          mongoc_collection_destroy(collection);
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          throw se;
        }
        if (bson_iter_init_find(&iter, doc, "username")) {
          new_user_mention.username = bson_iter_value(&iter)->value.v_utf8.str;
        } else {
          ServiceException se;
          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
          se.message = "Attribute of MongoDB item is not complete";
          bson_destroy(query);
          mongoc_cursor_destroy(cursor);
          mongoc_collection_destroy(collection);
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          throw se;
        }
        user_mentions.emplace_back(new_user_mention);
      }
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

      elapsed_time = time_us() - start_time;
      if (elapsed_time > 10000) { // 10ms
        LOG(info) << "Reading " << usernames_not_cached.size() << " keys from MongoDB uses " << elapsed_time << "us";
      }
    }
  }

  // Upload to compose post service
  auto compose_post_client_wrapper = _compose_client_pool->Pop();
  if (!compose_post_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to compose-post-service";
    throw se;
  }
  auto compose_post_client = compose_post_client_wrapper->GetClient();
  {
    auto rpc_trace_guard = _compose_client_pool->StartRpcTrace("UploadUserMentions", compose_post_client_wrapper);
    try {
      compose_post_client->UploadUserMentions(req_id, user_mentions,
                                              writer_text_map);
    } catch (...) {
      rpc_trace_guard->set_status(1);
      _compose_client_pool->Remove(compose_post_client_wrapper);
      LOG(error) << "Failed to upload user_mentions to user-mention-service";
      throw;
    }
  }
  _compose_client_pool->Push(compose_post_client_wrapper);
  span->Finish();
}

}

#endif //SOCIAL_NETWORK_MICROSERVICES_SRC_USERMENTIONSERVICE_USERMENTIONHANDLER_H_
