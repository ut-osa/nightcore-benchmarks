#ifndef MEDIA_MICROSERVICES_USERHANDLER_H
#define MEDIA_MICROSERVICES_USERHANDLER_H

#include <iostream>
#include <string>
#include <random>
#include <mongoc.h>
#include <bson/bson.h>
#include <iomanip>
#include <arpa/inet.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <nlohmann/json.hpp>
#include <jwt/jwt.hpp>

#include "../tracing.h"
#include "../../gen-cpp/UserService.h"
#include "../../gen-cpp/media_service_types.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../../gen-cpp/ComposeReviewService.h"
#include "../../third_party/PicoSHA2/picosha2.h"
#include "../logger.h"

// Custom Epoch (January 1, 2018 Midnight GMT = 2018-01-01T00:00:00Z)
#define CUSTOM_EPOCH 1514764800000

namespace media_service {

using std::chrono::milliseconds;
using std::chrono::duration_cast;
using std::chrono::system_clock;
//using namespace jwt::params;

static int64_t current_timestamp = -1;
static int counter = 0;

static int GetCounter(int64_t timestamp) {
  if (current_timestamp > timestamp) {
    LOG(fatal) << "Timestamps are not incremental.";
    exit(EXIT_FAILURE);
  }
  if (current_timestamp == timestamp) {
    return counter++;
  } else {
    current_timestamp = timestamp;
    counter = 0;
    return counter++;
  }
}

std::string GenRandomString(const int len) {
  static const std::string alphanum =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dist(
      0, static_cast<int>(alphanum.length() - 1));
  std::string s;
  for (int i = 0; i < len; ++i) {
    s += alphanum[dist(gen)];
  }
  return s;
}

class UserHandler : public UserServiceIf {
 public:
  UserHandler(
      std::mutex*,
      const std::string &,
      const std::string &,
      ClientPool<MCClient> *,
      mongoc_client_pool_t *,
      ClientPool<ThriftClient<ComposeReviewServiceClient>> *);
  ~UserHandler() override = default;
  void RegisterUser(
      int64_t,
      const std::string &,
      const std::string &,
      const std::string &,
      const std::string &,
      const std::map<std::string, std::string> &) override;
  void RegisterUserWithId(int64_t req_id, const std::string& first_name,
      const std::string& last_name, const std::string& username,
      const std::string& password, int64_t user_id,
      const std::map<std::string, std::string> & carrier) override;
  void UploadUserWithUserId(
      int64_t,
      int64_t,
      const std::map<std::string, std::string> &) override;
  void UploadUserWithUsername(
      int64_t,
      const std::string &,
      const std::map<std::string, std::string> &) override;
  void Login(
      std::string &,
      int64_t,
      const std::string &,
      const std::string &,
      const std::map<std::string, std::string> &) override;
 private:
  std::string _machine_id;
  std::string _secret;
  std::mutex *_thread_lock;
  ClientPool<MCClient> *_mc_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
  ClientPool<ThriftClient<ComposeReviewServiceClient>> *_compose_client_pool;

};

UserHandler::UserHandler(
    std::mutex *thread_lock,
    const std::string &machine_id,
    const std::string &secret,
    ClientPool<MCClient> *mc_client_pool,
    mongoc_client_pool_t *mongodb_client_pool,
    ClientPool<ThriftClient<ComposeReviewServiceClient>> *compose_client_pool
    ) {
  _thread_lock = thread_lock;
  _machine_id = machine_id;
  _mc_client_pool = mc_client_pool;
  _mongodb_client_pool = mongodb_client_pool;
  _compose_client_pool = compose_client_pool;
  _secret = secret;
}

void UserHandler::RegisterUser(
    const int64_t req_id,
    const std::string &first_name,
    const std::string &last_name,
    const std::string &username,
    const std::string &password,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "RegisterUser",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  // Compose user_id
  _thread_lock->lock();
  int64_t timestamp = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()).count() - CUSTOM_EPOCH;
  int counter = GetCounter(timestamp);
  _thread_lock->unlock();

  std::stringstream sstream;
  sstream << std::hex << timestamp;
  std::string timestamp_hex(sstream.str());
  if (timestamp_hex.size() > 10) {
    timestamp_hex.erase(0, timestamp_hex.size() - 10);
  } else if (timestamp_hex.size() < 10) {
    timestamp_hex =
        std::string(10 - timestamp_hex.size(), '0') + timestamp_hex;
  }

  // Empty the sstream buffer.
  sstream.clear();
  sstream.str(std::string());

  sstream << std::hex << counter;
  std::string counter_hex(sstream.str());

  if (counter_hex.size() > 3) {
    counter_hex.erase(0, counter_hex.size() - 3);
  } else if (counter_hex.size() < 3) {
    counter_hex = std::string(3 - counter_hex.size(), '0') + counter_hex;
  }
  std::string user_id_str = _machine_id + timestamp_hex + counter_hex;
  int64_t user_id = stoul(user_id_str, nullptr, 16) & 0x7FFFFFFFFFFFFFFF;
  LOG(debug) << "The user_id of the request " << req_id << " is " << user_id;

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

  // Check if the username has existed in the database
  bson_t *query = bson_new();
  BSON_APPEND_UTF8(query, "username", username.c_str());
  mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
      collection, query, nullptr, nullptr);
  const bson_t *doc;
  if (mongoc_cursor_next(cursor, &doc)) {
    bson_error_t error;
    if (mongoc_cursor_error (cursor, &error)) {
      LOG(warning) << error.message;
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = error.message;
      throw se;
    } else {
      LOG(warning) << "User " << username << " already existed.";
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
      se.message = "User " + username + " already existed";
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }
  } else {
    bson_t *new_doc = bson_new();
    BSON_APPEND_INT64(new_doc, "user_id", user_id);
    BSON_APPEND_UTF8(new_doc, "first_name", first_name.c_str());
    BSON_APPEND_UTF8(new_doc, "last_name", last_name.c_str());
    BSON_APPEND_UTF8(new_doc, "username", username.c_str());
    std::string salt = GenRandomString(32);
    BSON_APPEND_UTF8(new_doc, "salt", salt.c_str());
    std::string password_hashed = picosha2::hash256_hex_string(password + salt);
    BSON_APPEND_UTF8(new_doc, "password", password_hashed.c_str());

    bson_error_t error;
    auto user_insert_span = opentracing::Tracer::Global()->StartSpan(
        "MongoInsertUser", { opentracing::ChildOf(&span->context()) });
    if (!mongoc_collection_insert_one(
        collection, new_doc, nullptr, nullptr, &error)) {
      LOG(error) << "Failed to insert user " << username
          << " to MongoDB: " << error.message;
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
      se.message = "Failed to insert user " + username + " to MongoDB: "
          + error.message;
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    } else {
      LOG(info) << "User: " << username << " registered";
    }
    user_insert_span->Finish();
    bson_destroy(new_doc);
  }
  mongoc_cursor_destroy(cursor);
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

  span->Finish();
}

void UserHandler::RegisterUserWithId(
    int64_t req_id, const std::string& first_name,
    const std::string& last_name, const std::string& username,
    const std::string& password, int64_t user_id,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "RegisterUserWithId",
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
      mongodb_client, "user", "user");

  // Check if the username has existed in the database
  bson_t *query = bson_new();
  BSON_APPEND_UTF8(query, "username", username.c_str());
  mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
      collection, query, nullptr, nullptr);
  const bson_t *doc;
  if (mongoc_cursor_next(cursor, &doc)) {
    bson_error_t error;
    if (mongoc_cursor_error (cursor, &error)) {
      LOG(warning) << error.message;
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = error.message;
      throw se;
    } else {
      LOG(warning) << "User " << username << " already existed.";
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
      se.message = "User " + username + " already existed";
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }
  } else {
    bson_t *new_doc = bson_new();
    BSON_APPEND_INT64(new_doc, "user_id", user_id);
    BSON_APPEND_UTF8(new_doc, "first_name", first_name.c_str());
    BSON_APPEND_UTF8(new_doc, "last_name", last_name.c_str());
    BSON_APPEND_UTF8(new_doc, "username", username.c_str());
    std::string salt = GenRandomString(32);
    BSON_APPEND_UTF8(new_doc, "salt", salt.c_str());
    std::string password_hashed = picosha2::hash256_hex_string(password + salt);
    BSON_APPEND_UTF8(new_doc, "password", password_hashed.c_str());

    bson_error_t error;
    auto user_insert_span = opentracing::Tracer::Global()->StartSpan(
        "MongoInsertUser", { opentracing::ChildOf(&span->context()) });
    if (!mongoc_collection_insert_one(
        collection, new_doc, nullptr, nullptr, &error)) {
      LOG(error) << "Failed to insert user " << username
                 << " to MongoDB: " << error.message;
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
      se.message = "Failed to insert user " + username + " to MongoDB: "
          + error.message;
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    } else {
      LOG(info) << "User: " << username << " registered";
    }
    user_insert_span->Finish();
    bson_destroy(new_doc);
  }
  mongoc_cursor_destroy(cursor);
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

  span->Finish();
}

void UserHandler::UploadUserWithUsername(
    const int64_t req_id,
    const std::string &username,
    const std::map<std::string, std::string> & carrier) {

  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadUserWithUsername",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  size_t user_id_size;
  uint32_t memcached_flags;

  auto mc_client = _mc_client_pool->Pop();
  if (!mc_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  auto id_get_span = opentracing::Tracer::Global()->StartSpan(
      "MmcGetUserId", { opentracing::ChildOf(&span->context()) });
  std::string user_id_mmc;
  bool found_user_id_mmc;
  bool success = mc_client->Get(username+":user_id", &found_user_id_mmc, &user_id_mmc, &memcached_flags);
  id_get_span->Finish();
  if (!success) {
    _mc_client_pool->Push(mc_client);
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Cannot get user_id of request " + std::to_string(req_id);
    throw se;
  }
  _mc_client_pool->Push(mc_client);

  int64_t user_id = 0;

  if (found_user_id_mmc) {
    LOG(debug) << "Found password, salt and ID are cached in Memcached";
    user_id = std::stoul(user_id_mmc);
  }

  // If not cached in memcached
  else {
    LOG(debug) << "User_id not cached in Memcached";
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
      throw se;
    }
    bson_t *query = bson_new();
    BSON_APPEND_UTF8(query, "username", username.c_str());

    auto find_span = opentracing::Tracer::Global()->StartSpan(
        "MongoFindUser", { opentracing::ChildOf(&span->context()) });
    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
        collection, query, nullptr, nullptr);
    const bson_t *doc;
    bool found = mongoc_cursor_next(cursor, &doc);
    find_span->Finish();

    if (!found) {
      bson_error_t error;
      if (mongoc_cursor_error (cursor, &error)) {
        LOG(warning) << error.message;
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        ServiceException se;
        se.errorCode = ErrorCode::SE_MONGODB_ERROR;
        se.message = error.message;
        throw se;
      } else {
        LOG(warning) << "User: " << username << " doesn't exist in MongoDB";
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        ServiceException se;
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message = "User: " + username + " is not registered";
        throw se;
      }
    } else {
      LOG(debug) << "User: " << username << " found in MongoDB";
      bson_iter_t iter;
      if (bson_iter_init_find(&iter, doc, "user_id")) {
        user_id = bson_iter_value(&iter)->value.v_int64;
      } else {
        LOG(error) << "user_id attribute of user "
                   << username <<" was not found in the User object";
        bson_destroy(query);
        mongoc_cursor_destroy(cursor);
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        ServiceException se;
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message = "user_id attribute of user: " + username +
            " was not found in the User object";
        throw se;
      }
    }
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
  }

  if (user_id) {
    auto compose_client_wrapper = _compose_client_pool->Pop();
    if (!compose_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connected to compose-review-service";
      throw se;
    }
    auto compose_client = compose_client_wrapper->GetClient();
    try {
      compose_client->UploadUserId(req_id, user_id, writer_text_map);
    } catch (...) {
      _compose_client_pool->Remove(compose_client_wrapper);
      LOG(error) << "Failed to upload movie_id to compose-review-service";
      throw;
    }
    _compose_client_pool->Push(compose_client_wrapper);
  }

  mc_client = _mc_client_pool->Pop();
  if (!mc_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  if (user_id && !found_user_id_mmc) {
    auto id_set_span = opentracing::Tracer::Global()->StartSpan(
        "MmcSetUserId", { opentracing::ChildOf(&span->context()) });
    std::string user_id_str = std::to_string(user_id);
    bool success = mc_client->Set(username+":user_id", user_id_str, 0, 0);
    id_set_span->Finish();
    if (!success) {
      LOG(warning)
        << "Failed to set the user_id of user "
        << username << " to Memcached";
    }
  }
  _mc_client_pool->Push(mc_client);

  span->Finish();
}

void UserHandler::UploadUserWithUserId(
    int64_t req_id,
    int64_t user_id,
    const std::map<std::string, std::string> &carrier) {

  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadUserWithUserId",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto compose_client_wrapper = _compose_client_pool->Pop();
  if (!compose_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connected to compose-review-service";
    throw se;
  }
  auto compose_client = compose_client_wrapper->GetClient();
  try {
    compose_client->UploadUserId(req_id, user_id, writer_text_map);
  } catch (...) {
    _compose_client_pool->Remove(compose_client_wrapper);
    LOG(error) << "Failed to upload movie_id to compose-review-service";
    throw;
  }
  _compose_client_pool->Push(compose_client_wrapper);

  span->Finish();

}


void UserHandler::Login(
    std::string & _return,
    int64_t req_id,
    const std::string &username,
    const std::string &password,
    const std::map<std::string, std::string> &carrier) {

  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "Login",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  uint32_t memcached_flags;

  auto mc_client = _mc_client_pool->Pop();
  if (!mc_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  auto pswd_get_span = opentracing::Tracer::Global()->StartSpan(
      "MmcGetPassword", { opentracing::ChildOf(&span->context()) });
  std::string password_mmc;
  bool found_password_mmc;
  bool success = mc_client->Get(username+":password", &found_password_mmc, &password_mmc, &memcached_flags);
  pswd_get_span->Finish();
  if (!success) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Cannot get password of request " + std::to_string(req_id);
    _mc_client_pool->Push(mc_client);
    throw se;
  }

  auto salt_get_span = opentracing::Tracer::Global()->StartSpan(
      "MmcGetSalt", { opentracing::ChildOf(&span->context()) });
  std::string salt_mmc;
  bool found_salt_mmc;
  success = mc_client->Get(username+":salt", &found_salt_mmc, &salt_mmc, &memcached_flags);
  salt_get_span->Finish();
  if (!success) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Cannot get salt of request " + std::to_string(req_id);
    _mc_client_pool->Push(mc_client);
    throw se;
  }

  auto id_get_span = opentracing::Tracer::Global()->StartSpan(
      "MmcGetUserId", { opentracing::ChildOf(&span->context()) });
  std::string user_id_mmc;
  bool found_user_id_mmc;
  success = mc_client->Get(username+":user_id", &found_user_id_mmc, &user_id_mmc, &memcached_flags);
  id_get_span->Finish();
  if (!success) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Cannot get user_id of request " + std::to_string(req_id);
    _mc_client_pool->Push(mc_client);
    throw se;
  }

  _mc_client_pool->Push(mc_client);

  int64_t user_id = 0;
  std::string salt_str;
  std::string password_str;

  if (found_password_mmc && found_salt_mmc && found_user_id_mmc) {
    LOG(debug) << "Found password, salt and ID are cached in Memcached";
    user_id = std::stoul(user_id_mmc);
    password_str = password_mmc;
    salt_str = salt_mmc;
  }

    // If not cached in memcached
  else {
    LOG(debug) << "Password or salt or ID not cached in Memcached";
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
      throw se;
    }
    bson_t *query = bson_new();
    BSON_APPEND_UTF8(query, "username", username.c_str());

    auto find_span = opentracing::Tracer::Global()->StartSpan(
        "MongoFindUser", { opentracing::ChildOf(&span->context()) });
    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
        collection, query, nullptr, nullptr);
    const bson_t *doc;
    bool found = mongoc_cursor_next(cursor, &doc);
    find_span->Finish();

    if (!found) {
      bson_error_t error;
      if (mongoc_cursor_error (cursor, &error)) {
        LOG(warning) << error.message;
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        ServiceException se;
        se.errorCode = ErrorCode::SE_MONGODB_ERROR;
        se.message = error.message;
        throw se;
      } else {
        LOG(warning) << "User: " << username << " doesn't exist in MongoDB";
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        ServiceException se;
        se.errorCode = ErrorCode::SE_UNAUTHORIZED;
        se.message = "User: " + username + " is not registered";
        throw se;
      }
    } else {
      LOG(debug) << "User: " << username << " found in MongoDB";
      if (!found_password_mmc) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "password")) {
          password_str = bson_iter_value(&iter)->value.v_utf8.str;
        } else {
          LOG(error) << "Password attribute of user "
                     << username <<" was not found in the User object";
          bson_destroy(query);
          mongoc_cursor_destroy(cursor);
          mongoc_collection_destroy(collection);
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
          se.message = "Password attribute of user: " + username +
              " was not found in the User object";
          throw se;
        }
      }

      if (!found_salt_mmc) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "salt")) {
          salt_str = bson_iter_value(&iter)->value.v_utf8.str;
        } else {
          LOG(error) << "Salt attribute of user "
                     << username <<" was not found in the User object";
          bson_destroy(query);
          mongoc_cursor_destroy(cursor);
          mongoc_collection_destroy(collection);
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
          se.message = "Salt attribute of user: " + username +
              " was not found in the User object";
          throw se;
        }
      }

      if (!found_user_id_mmc) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "user_id")) {
          user_id = bson_iter_value(&iter)->value.v_int64;
        } else {
          LOG(error) << "user_Id attribute of user "
                     << username <<" was not found in the User object";
          bson_destroy(query);
          mongoc_cursor_destroy(cursor);
          mongoc_collection_destroy(collection);
          mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
          se.message = "User_id attribute of user: " + username +
              " was not found in the User object";
          throw se;
        }
      }
    }

    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
  }

  if (user_id && !salt_str.empty() && !password_str.empty()) {
    bool auth = picosha2::hash256_hex_string(password + std::string(salt_str))
        == std::string(password_str);
    if (auth) {
      auto user_id_str = std::to_string(user_id);
      auto timestamp_str = std::to_string(duration_cast<milliseconds>(
          system_clock::now().time_since_epoch()).count());


      jwt::jwt_object obj{
          jwt::params::algorithm("HS256"),
          jwt::params::secret(_secret),
          jwt::params::payload({
              {"user_id", user_id_str},
              {"timestamp", timestamp_str},
              {"TTL", "60000"}
          })
      };
      _return = obj.signature();

    } else {
      ServiceException se;
      se.errorCode = ErrorCode::SE_UNAUTHORIZED;
      se.message = "Incorrect username or password";
      throw se;
    }
  }

  mc_client = _mc_client_pool->Pop();
  if (!mc_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  if (!salt_str.empty() && !found_salt_mmc) {
    auto salt_set_span = opentracing::Tracer::Global()->StartSpan(
        "MmcSetSalt", { opentracing::ChildOf(&span->context()) });
    bool success = mc_client->Set(username+":salt", salt_str, 0, 0);
    salt_set_span->Finish();

    if (!success) {
      LOG(warning)
        << "Failed to set the salt of user "
        << username << " to Memcached";
    }
  }

  if (!password_str.empty() && !found_password_mmc) {
    auto pswd_set_span = opentracing::Tracer::Global()->StartSpan(
        "MmcSetPassword", { opentracing::ChildOf(&span->context()) });
    bool success = mc_client->Set(username+":password", password_str, 0, 0);
    pswd_set_span->Finish();
    if (!success) {
      LOG(warning)
        << "Failed to set the password of user "
        << username << " to Memcached";
    }
  }

  if (user_id && !found_user_id_mmc) {
    auto id_set_span = opentracing::Tracer::Global()->StartSpan(
        "MmcSetUserId", { opentracing::ChildOf(&span->context()) });
    std::string user_id_str = std::to_string(user_id);
    bool success = mc_client->Set(username+":user_id", user_id_str, 0, 0);
    id_set_span->Finish();
    if (!success) {
      LOG(warning)
        << "Failed to set the user_id of user "
        << username << " to Memcached";
    }
  }
  _mc_client_pool->Push(mc_client);

  span->Finish();
}

/*
 * The following code which obtaines machine ID from machine's MAC address was
 * inspired from https://stackoverflow.com/a/16859693.
 */
u_int16_t HashMacAddressPid(const std::string &mac)
{
  u_int16_t hash = 0;
  std::string mac_pid = mac + std::to_string(getpid());
  for ( unsigned int i = 0; i < mac_pid.size(); i++ ) {
    hash += ( mac_pid[i] << (( i & 1 ) * 8 ));
  }
  return hash;
}

int GetMachineId (std::string *mac_hash) {
  std::string mac;
  int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP );
  if ( sock < 0 ) {
    LOG(error) << "Unable to obtain MAC address";
    return -1;
  }

  struct ifconf conf{};
  char ifconfbuf[ 128 * sizeof(struct ifreq)  ];
  memset( ifconfbuf, 0, sizeof( ifconfbuf ));
  conf.ifc_buf = ifconfbuf;
  conf.ifc_len = sizeof( ifconfbuf );
  if ( ioctl( sock, SIOCGIFCONF, &conf ))
  {
    LOG(error) << "Unable to obtain MAC address";
    return -1;
  }

  struct ifreq* ifr;
  for (
      ifr = conf.ifc_req;
      reinterpret_cast<char *>(ifr) <
          reinterpret_cast<char *>(conf.ifc_req) + conf.ifc_len;
      ifr++) {
    if ( ifr->ifr_addr.sa_data == (ifr+1)->ifr_addr.sa_data ) {
      continue;  // duplicate, skip it
    }

    if ( ioctl( sock, SIOCGIFFLAGS, ifr )) {
      continue;  // failed to get flags, skip it
    }
    if ( ioctl( sock, SIOCGIFHWADDR, ifr ) == 0 ) {
      mac = std::string(ifr->ifr_addr.sa_data);
      if (!mac.empty()) {
        break;
      }
    }
  }
  close(sock);

  std::stringstream stream;
  stream << std::hex << HashMacAddressPid(mac);
  *mac_hash = stream.str();

  if (mac_hash->size() > 3) {
    mac_hash->erase(0, mac_hash->size() - 3);
  } else if (mac_hash->size() < 3) {
    *mac_hash = std::string(3 - mac_hash->size(), '0') + *mac_hash;
  }
  return 0;
}
} // namespace media_service

#endif //MEDIA_MICROSERVICES_USERHANDLER_H



