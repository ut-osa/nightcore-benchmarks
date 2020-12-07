#ifndef MEDIA_MICROSERVICES_MOVIEREVIEWHANDLER_H
#define MEDIA_MICROSERVICES_MOVIEREVIEWHANDLER_H

#include <iostream>
#include <string>

#include <mongoc.h>
#include <bson/bson.h>

#include "../../gen-cpp/MovieReviewService.h"
#include "../../gen-cpp/ReviewStorageService.h"
#include "../logger.h"
#include "../tracing.h"
#include "../ClientPool.h"
#include "../RedisClient.h"
#include "../ThriftClient.h"

namespace media_service {
class MovieReviewHandler : public MovieReviewServiceIf {
 public:
  MovieReviewHandler(
      ClientPool<RedisClient> *,
      mongoc_client_pool_t *,
      ClientPool<ThriftClient<ReviewStorageServiceClient>> *);
  ~MovieReviewHandler() override = default;
  void UploadMovieReview(int64_t, const std::string&, int64_t, int64_t,
                         const std::map<std::string, std::string> &) override;
  void ReadMovieReviews(std::vector<Review> & _return, int64_t req_id,
      const std::string& movie_id, int32_t start, int32_t stop, 
      const std::map<std::string, std::string> & carrier) override;
  
 private:
  ClientPool<RedisClient> *_redis_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
  ClientPool<ThriftClient<ReviewStorageServiceClient>> *_review_client_pool;
};

MovieReviewHandler::MovieReviewHandler(
    ClientPool<RedisClient> *redis_client_pool,
    mongoc_client_pool_t *mongodb_pool,
    ClientPool<ThriftClient<ReviewStorageServiceClient>> *review_storage_client_pool) {
  _redis_client_pool = redis_client_pool;
  _mongodb_client_pool = mongodb_pool;
  _review_client_pool = review_storage_client_pool;
}

void MovieReviewHandler::UploadMovieReview(
    int64_t req_id,
    const std::string& movie_id,
    int64_t review_id,
    int64_t timestamp,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadMovieReview",
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
      mongodb_client, "movie-review", "movie-review");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection movie-review from DB movie-review";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }

  bson_t *query = bson_new();
  BSON_APPEND_UTF8(query, "movie_id", movie_id.c_str());
  auto find_span = opentracing::Tracer::Global()->StartSpan(
      "MongoFindMovie", {opentracing::ChildOf(&span->context())});
  mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
      collection, query, nullptr, nullptr);
  const bson_t *doc;
  bool found = mongoc_cursor_next(cursor, &doc);
  if (!found) {
    bson_t *new_doc = BCON_NEW(
        "movie_id", BCON_UTF8(movie_id.c_str()),
        "reviews",
        "[", "{", "review_id", BCON_INT64(review_id),
        "timestamp", BCON_INT64(timestamp), "}", "]"
    );
    bson_error_t error;
    auto insert_span = opentracing::Tracer::Global()->StartSpan(
        "MongoInsert", {opentracing::ChildOf(&span->context())});
    bool plotinsert = mongoc_collection_insert_one(
        collection, new_doc, nullptr, nullptr, &error);
    insert_span->Finish();
    if (!plotinsert) {
      LOG(error) << "Failed to insert movie review of movie " << movie_id
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
        "reviews", "{",
        "$each", "[", "{",
        "review_id", BCON_INT64(review_id),
        "timestamp", BCON_INT64(timestamp),
        "}", "]",
        "$position", BCON_INT32(0),
        "}",
        "}"
    );
    bson_error_t error;
    bson_t reply;
    auto update_span = opentracing::Tracer::Global()->StartSpan(
        "MongoUpdate.", {opentracing::ChildOf(&span->context())});
    bool plotupdate = mongoc_collection_find_and_modify(
        collection, query, nullptr, update, nullptr, false, false,
        true, &reply, &error);
    update_span->Finish();
    if (!plotupdate) {
      LOG(error) << "Failed to update movie-review for movie " << movie_id
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
    se.message = "Cannot connected to Redis server";
    throw se;
  }
  auto redis_client = redis_client_wrapper->GetClient();
  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "RedisUpdate", {opentracing::ChildOf(&span->context())});
  redis_client.AppendCommand("ZCARD %" PRId64, movie_id);
  auto num_reviews_reply = redis_client.GetReply();
  std::vector<std::string> options{"NX"};
  num_reviews_reply->check_ok();
  if (num_reviews_reply->as_integer()) {
    redis_client.AppendCommand("ZADD %" PRId64 " NX %" PRId64 " %" PRId64, movie_id, timestamp, review_id);
    auto zadd_reply = redis_client.GetReply();
    zadd_reply->check_ok();
  }
  _redis_client_pool->Push(redis_client_wrapper);
  redis_span->Finish();
  span->Finish();
}

void MovieReviewHandler::ReadMovieReviews(
    std::vector<Review> & _return, int64_t req_id,
    const std::string& movie_id, int32_t start, int32_t stop,
    const std::map<std::string, std::string> & carrier) {
  
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "ReadMovieReviews",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  if (stop <= start || start < 0) {
    return;
  }

  auto redis_client_wrapper = _redis_client_pool->Pop();
  if (!redis_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_REDIS_ERROR;
    se.message = "Cannot connected to Redis server";
    throw se;
  }
  auto redis_client = redis_client_wrapper->GetClient();
  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "RedisFind", {opentracing::ChildOf(&span->context())});
  redis_client.AppendCommand("ZREVRANGE %" PRId64 " %d %d", movie_id, start, stop - 1);
  auto review_ids_reply = redis_client.GetReply();
  _redis_client_pool->Push(redis_client_wrapper);
  redis_span->Finish();

  std::vector<int64_t> review_ids;
  auto review_ids_reply_array = review_ids_reply->as_array();
  for (auto &review_id_reply : review_ids_reply_array) {
    review_ids.emplace_back(std::stoul(review_id_reply->as_string()));
  }

  int mongo_start = start + review_ids.size();
  std::multimap<std::string, std::string> redis_update_map;
  if (mongo_start < stop) {
    // Instead find review_ids from mongodb
    mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
        _mongodb_client_pool);
    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }
    auto collection = mongoc_client_get_collection(
        mongodb_client, "movie-review", "movie-review");
    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection movie-review from MongoDB";
      throw se;
    }

    bson_t *query = BCON_NEW("movie_id", BCON_UTF8(movie_id.c_str()));
    bson_t *opts = BCON_NEW(
        "projection", "{",
        "reviews", "{",
        "$slice", "[",
        BCON_INT32(0), BCON_INT32(stop),
        "]", "}", "}");
    auto find_span = opentracing::Tracer::Global()->StartSpan(
        "MongoFindMovieReviews", {opentracing::ChildOf(&span->context())});
    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
        collection, query, opts, nullptr);
    find_span->Finish();
    const bson_t *doc;
    bool found = mongoc_cursor_next(cursor, &doc);
    if (found) {
      bson_iter_t iter_0;
      bson_iter_t iter_1;
      bson_iter_t review_id_child;
      bson_iter_t timestamp_child;
      int idx = 0;
      bson_iter_init(&iter_0, doc);
      bson_iter_init(&iter_1, doc);
      while (bson_iter_find_descendant(&iter_0,
                                       ("reviews." + std::to_string(idx)
                                           + ".review_id").c_str(),
                                       &review_id_child)
          && BSON_ITER_HOLDS_INT64(&review_id_child)
          && bson_iter_find_descendant(&iter_1,
                                       ("reviews." + std::to_string(idx)
                                           + ".timestamp").c_str(),
                                       &timestamp_child)
          && BSON_ITER_HOLDS_INT64(&timestamp_child)) {
        auto curr_review_id = bson_iter_int64(&review_id_child);
        auto curr_timestamp = bson_iter_int64(&timestamp_child);
        if (idx >= mongo_start) {
          review_ids.emplace_back(curr_review_id);
        }
        redis_update_map.insert(
            {std::to_string(curr_timestamp), std::to_string(curr_review_id)});
        bson_iter_init(&iter_0, doc);
        bson_iter_init(&iter_1, doc);
        idx++;
      }
    }
    find_span->Finish();
    bson_destroy(opts);
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
  }

  // std::future<std::vector<Review>> review_future = std::async(
  //     std::launch::async, [&]() {
        auto review_client_wrapper = _review_client_pool->Pop();
        if (!review_client_wrapper) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
          se.message = "Failed to connected to review-storage-service";
          throw se;
        }
        std::vector<Review> _return_reviews;
        auto review_client = review_client_wrapper->GetClient();
        try {
          review_client->ReadReviews(
              _return_reviews, req_id, review_ids, writer_text_map);
        } catch (...) {
          _review_client_pool->Remove(review_client_wrapper);
          LOG(error) << "Failed to read review from review-storage-service";
          throw;
        }
        _review_client_pool->Push(review_client_wrapper);
        _return = _return_reviews;
        // return _return_reviews;
      // });
  
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
    redis_client.AppendCommand("DEL %" PRId64, movie_id);
    std::stringstream cmd;
    cmd << "ZADD " << movie_id << " NX";
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
  //   _return = review_future.get();
  // } catch (...) {
  //   LOG(error) << "Failed to get review from review-storage-service";
  //   if (!redis_update_map.empty()) {
  //     try {
  //       zadd_reply_future.get();
  //     } catch (...) {
  //       LOG(error) << "Failed to Update Redis Server";
  //     }
  //     _redis_client_pool->Push(redis_client_wrapper);
  //   }
  //   throw;
  // }

  // if (!redis_update_map.empty()) {
  //   try {
  //     zadd_reply_future.get();
  //   } catch (...) {
  //     LOG(error) << "Failed to Update Redis Server";
  //     _redis_client_pool->Push(redis_client_wrapper);
  //     throw;
  //   }
  //   _redis_client_pool->Push(redis_client_wrapper);
  // }

  span->Finish();
  
}

} // namespace media_service


#endif //MEDIA_MICROSERVICES_MOVIEREVIEWHANDLER_H
