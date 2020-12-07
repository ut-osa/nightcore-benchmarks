#ifndef MEDIA_MICROSERVICES_MOVIEIDHANDLER_H
#define MEDIA_MICROSERVICES_MOVIEIDHANDLER_H

#include <iostream>
#include <string>
#include <future>

#include <mongoc.h>
#include <bson/bson.h>

#include "../../gen-cpp/MovieIdService.h"
#include "../../gen-cpp/ComposeReviewService.h"
#include "../../gen-cpp/RatingService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"


namespace media_service {

static const std::string base64_chars = 
             "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
             "abcdefghijklmnopqrstuvwxyz"
             "0123456789+/";


static inline bool is_base64(unsigned char c) {
  return (isalnum(c) || (c == '+') || (c == '/'));
}

std::string base64_encode(const std::string& in) {
  std::string ret;
  int i = 0;
  int j = 0;
  unsigned char char_array_3[3];
  unsigned char char_array_4[4];

  char const* bytes_to_encode = in.data();
  unsigned int in_len = in.length();

  while (in_len--) {
    char_array_3[i++] = *(bytes_to_encode++);
    if (i == 3) {
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for(i = 0; (i <4) ; i++)
        ret += base64_chars[char_array_4[i]];
      i = 0;
    }
  }

  if (i)
  {
    for(j = i; j < 3; j++)
      char_array_3[j] = '\0';

    char_array_4[0] = ( char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);

    for (j = 0; (j < i + 1); j++)
      ret += base64_chars[char_array_4[j]];

    while((i++ < 3))
      ret += '=';

  }

  return ret;

}


class MovieIdHandler : public MovieIdServiceIf {
 public:
  MovieIdHandler(
      ClientPool<MCClient> *,
      mongoc_client_pool_t *,
      ClientPool<ThriftClient<ComposeReviewServiceClient>> *,
      ClientPool<ThriftClient<RatingServiceClient>> *);
  ~MovieIdHandler() override = default;
  void UploadMovieId(int64_t, const std::string &, int32_t,
                     const std::map<std::string, std::string> &) override;
  void RegisterMovieId(int64_t, const std::string &, const std::string &,
                       const std::map<std::string, std::string> &) override;

 private:
  ClientPool<MCClient> *_mc_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
  ClientPool<ThriftClient<ComposeReviewServiceClient>> *_compose_client_pool;
  ClientPool<ThriftClient<RatingServiceClient>> *_rating_client_pool;
};

MovieIdHandler::MovieIdHandler(
    ClientPool<MCClient> *mc_client_pool,
    mongoc_client_pool_t *mongodb_client_pool,
    ClientPool<ThriftClient<ComposeReviewServiceClient>> *compose_client_pool,
    ClientPool<ThriftClient<RatingServiceClient>> *rating_client_pool) {
  _mc_client_pool = mc_client_pool;
  _mongodb_client_pool = mongodb_client_pool;
  _compose_client_pool = compose_client_pool;
  _rating_client_pool = rating_client_pool;
}

void MovieIdHandler::UploadMovieId(
    int64_t req_id,
    const std::string &title,
    int32_t rating,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadMovieId",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto mc_client = _mc_client_pool->Pop();
  if (!mc_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  size_t movie_id_size;
  uint32_t memcached_flags;
  // Look for the movie id from memcached

  auto get_span = opentracing::Tracer::Global()->StartSpan(
      "MmcGetMovieId", { opentracing::ChildOf(&span->context()) });
  
  bool found;
  std::string movie_id_mmc;
  bool success = mc_client->Get(base64_encode(title), &found, &movie_id_mmc, &memcached_flags);

  if (!success) {
    _mc_client_pool->Push(mc_client);
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to get movie_id from memcached";
    LOG(error) << "Failed to get movie_id from memcached";
    throw se;
  }
  get_span->Finish();
  _mc_client_pool->Push(mc_client);
  std::string movie_id_str;

  // If cached in memcached
  if (found) {
    LOG(debug) << "Get movie_id " << movie_id_mmc
        << " cache hit from Memcached";
    movie_id_str = std::string(movie_id_mmc);
  }

    // If not cached in memcached
  else {
    mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
        _mongodb_client_pool);
    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }
    auto collection = mongoc_client_get_collection(
        mongodb_client, "movie-id", "movie-id");

    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection user from DB movie-id";
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }

    bson_t *query = bson_new();
    BSON_APPEND_UTF8(query, "title", title.c_str());

    auto find_span = opentracing::Tracer::Global()->StartSpan(
        "MongoFindMovieId", { opentracing::ChildOf(&span->context()) });
    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
        collection, query, nullptr, nullptr);
    const bson_t *doc;
    bool found = mongoc_cursor_next(cursor, &doc);
    find_span->Finish();

    if (found) {
      bson_iter_t iter;
      if (bson_iter_init_find(&iter, doc, "movie_id")) {
        movie_id_str = std::string(bson_iter_value(&iter)->value.v_utf8.str);
        LOG(debug) << "Find movie " << movie_id_str << " cache miss";
      } else {
        LOG(error) << "Attribute movie_id is not find in MongoDB";
        bson_destroy(query);
        mongoc_cursor_destroy(cursor);
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        ServiceException se;
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message = "Attribute movie_id is not find in MongoDB";
        throw se;
      }
    } else {
      LOG(error) << "Movie " << title << " is not found in MongoDB";
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
      se.message = "Movie " + title + " is not found in MongoDB";
      throw se;
    }
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
  }
  
  // std::future<void> set_future;
  // std::future<void> movie_id_future;
  // std::future<void> rating_future;
  // set_future = std::async(std::launch::async, [&]() {
    mc_client = _mc_client_pool->Pop();
    if (!mc_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message = "Failed to pop a client from memcached pool";
      throw se;
    }
    auto set_span = opentracing::Tracer::Global()->StartSpan(
        "MmcSetMovieId", { opentracing::ChildOf(&span->context()) });
    // Upload the movie id to memcached
    success = mc_client->Set(base64_encode(title), movie_id_str, 0, 0);
    set_span->Finish();
    if (!success) {
      LOG(warning) << "Failed to set movie_id to Memcached";
    }
    _mc_client_pool->Push(mc_client);
  // });

  // movie_id_future = std::async(std::launch::async, [&]() {
    auto compose_client_wrapper = _compose_client_pool->Pop();
    if (!compose_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connected to compose-review-service";
      throw se;
    }
    auto compose_client = compose_client_wrapper->GetClient();
    try {
      compose_client->UploadMovieId(req_id, movie_id_str, writer_text_map);
    } catch (...) {
      _compose_client_pool->Remove(compose_client_wrapper);
      LOG(error) << "Failed to upload movie_id to compose-review-service";
      throw;
    }
    _compose_client_pool->Push(compose_client_wrapper);
  // });

  // rating_future = std::async(std::launch::async, [&]() {
    auto rating_client_wrapper = _rating_client_pool->Pop();
    if (!rating_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connected to rating-service";
      throw se;
    }
    auto rating_client = rating_client_wrapper->GetClient();
    try {
      rating_client->UploadRating(req_id, movie_id_str, rating, writer_text_map);
    } catch (...) {
      _rating_client_pool->Remove(rating_client_wrapper);
      LOG(error) << "Failed to upload rating to rating-service";
      throw;
    }
    _rating_client_pool->Push(rating_client_wrapper);
  // });

  // try {
  //   movie_id_future.get();
  //   rating_future.get();
  //   set_future.get();
  // } catch (...) {
  //   throw;
  // }

  span->Finish();
}

void MovieIdHandler::RegisterMovieId (
    const int64_t req_id,
    const std::string &title,
    const std::string &movie_id,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "RegisterMovieId",
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
      mongodb_client, "movie-id", "movie-id");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection movie_id from DB movie-id";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }

  // Check if the username has existed in the database
  bson_t *query = bson_new();
  BSON_APPEND_UTF8(query, "title", title.c_str());

  auto find_span = opentracing::Tracer::Global()->StartSpan(
      "MongoFindMovie", { opentracing::ChildOf(&span->context()) });
  mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
      collection, query, nullptr, nullptr);
  const bson_t *doc;
  bool found = mongoc_cursor_next(cursor, &doc);
  find_span->Finish();

  if (found) {
    LOG(warning) << "Movie "<< title << " already existed in MongoDB";
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "Movie " + title + " already existed in MongoDB";
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  } else {
    bson_t *new_doc = bson_new();
    BSON_APPEND_UTF8(new_doc, "title", title.c_str());
    BSON_APPEND_UTF8(new_doc, "movie_id", movie_id.c_str());
    bson_error_t error;

    auto insert_span = opentracing::Tracer::Global()->StartSpan(
        "MongoInsertMovie", { opentracing::ChildOf(&span->context()) });
    bool plotinsert = mongoc_collection_insert_one (
        collection, new_doc, nullptr, nullptr, &error);
    insert_span->Finish();

    if (!plotinsert) {
      LOG(error) << "Failed to insert movie_id of " << title
          << " to MongoDB: " << error.message;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = error.message;
      bson_destroy(new_doc);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }
    bson_destroy(new_doc);
  }
  mongoc_cursor_destroy(cursor);
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

  span->Finish();
}
} // namespace media_service

#endif //MEDIA_MICROSERVICES_MOVIEIDHANDLER_H
