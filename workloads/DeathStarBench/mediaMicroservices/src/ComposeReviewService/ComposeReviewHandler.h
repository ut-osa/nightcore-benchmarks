#ifndef MEDIA_MICROSERVICES_COMPOSEREVIEWHANDLER_H
#define MEDIA_MICROSERVICES_COMPOSEREVIEWHANDLER_H

#include <iostream>
#include <string>
#include <chrono>
#include <future>

#include "../../gen-cpp/ComposeReviewService.h"
#include "../../gen-cpp/media_service_types.h"
#include "../../gen-cpp/ReviewStorageService.h"
#include "../../gen-cpp/UserReviewService.h"
#include "../../gen-cpp/MovieReviewService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"

namespace media_service {
#define NUM_COMPONENTS 5
#define MMC_EXP_TIME 10

using std::chrono::milliseconds;
using std::chrono::duration_cast;
using std::chrono::system_clock;

class ComposeReviewHandler : public ComposeReviewServiceIf {
 public:
  ComposeReviewHandler(
      ClientPool<MCClient> *,
      ClientPool<ThriftClient<ReviewStorageServiceClient>> *,
      ClientPool<ThriftClient<UserReviewServiceClient>> *,
      ClientPool<ThriftClient<MovieReviewServiceClient>> *);
  ~ComposeReviewHandler() override = default;

  void UploadText(int64_t, const std::string &,
      const std::map<std::string, std::string> &) override;
  void UploadRating(int64_t, int32_t,
      const std::map<std::string, std::string> &) override;
  void UploadUniqueId(int64_t, int64_t,
      const std::map<std::string, std::string> &) override;
  void UploadMovieId(int64_t, const std::string &,
                     const std::map<std::string, std::string> &) override;
  void UploadUserId(int64_t, int64_t,
      const std::map<std::string, std::string> &) override;


 private:
  ClientPool<MCClient> *_mc_client_pool;
  ClientPool<ThriftClient<ReviewStorageServiceClient>>
      *_review_storage_client_pool;
  ClientPool<ThriftClient<UserReviewServiceClient>>
      *_user_review_client_pool;
  ClientPool<ThriftClient<MovieReviewServiceClient>>
      *_movie_review_client_pool;
  void _ComposeAndUpload(int64_t, const std::map<std::string, std::string> &);
};

ComposeReviewHandler::ComposeReviewHandler(
    ClientPool<MCClient> *mc_client_pool,
    ClientPool<ThriftClient<ReviewStorageServiceClient>> 
        *review_storage_client_pool,
    ClientPool<ThriftClient<UserReviewServiceClient>>
        *user_review_client_pool,
    ClientPool<ThriftClient<MovieReviewServiceClient>>
        *movie_review_client_pool ) {
  _mc_client_pool = mc_client_pool;
  _review_storage_client_pool = review_storage_client_pool;
  _user_review_client_pool = user_review_client_pool;
  _movie_review_client_pool = movie_review_client_pool;
}

void ComposeReviewHandler::_ComposeAndUpload(
    int64_t req_id, const std::map<std::string, std::string> &writer_text_map) {

  std::string key_unique_id = std::to_string(req_id) + ":review_id";
  std::string key_movie_id = std::to_string(req_id) + ":movie_id";
  std::string key_user_id = std::to_string(req_id) + ":user_id";
  std::string key_text = std::to_string(req_id) + ":text";
  std::string key_rating = std::to_string(req_id) + ":rating";

  const char* keys[NUM_COMPONENTS] = {
      key_unique_id.c_str(),
      key_movie_id.c_str(),
      key_user_id.c_str(),
      key_text.c_str(),
      key_rating.c_str()
  };

  // Compose a review from the components obtained from memcached
  auto mc_client = _mc_client_pool->Pop();
  if (!mc_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  Review new_review;
  bool found;
  std::string return_value;
  uint32_t flags;
  for (int i = 0; i < NUM_COMPONENTS; i++) {
    bool success = mc_client->Get(keys[i], &found, &return_value, &flags);
    if (!success) {
      _mc_client_pool->Push(mc_client);
      LOG(error) << "Cannot get components of request " << req_id;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message =  "Cannot get components of request " + std::to_string(req_id);
      throw se;
    }
    if (!found) continue;
    if (keys[i] == key_unique_id) {
      new_review.review_id = std::stoul(return_value);
    } else if (keys[i] == key_movie_id) {
      new_review.movie_id = return_value;
    } else if (keys[i] == key_text) {
      new_review.text = return_value;
    } else if (keys[i] == key_user_id) {
      new_review.user_id = std::stoul(return_value);
    } else if (keys[i] == key_rating) {
      new_review.rating = std::stoi(return_value);
    }
  }
  _mc_client_pool->Push(mc_client);

  new_review.timestamp = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()).count();
  new_review.req_id = req_id;

  std::future<void> review_future;
  std::future<void> user_review_future;
  std::future<void> movie_review_future;
  
  // review_future = std::async(std::launch::async, [&](){
    auto review_storage_client_wrapper = _review_storage_client_pool->Pop();
    if (!review_storage_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connected to review-storage-service";
      throw se;
    }
    auto review_storage_client = review_storage_client_wrapper->GetClient();
    try {
      review_storage_client->StoreReview(req_id, new_review, writer_text_map);
    } catch (...) {
      _review_storage_client_pool->Remove(review_storage_client_wrapper);
      LOG(error) << "Failed to upload review to review-storage-service";
      throw;
    }
    _review_storage_client_pool->Push(review_storage_client_wrapper);
  // });

  // user_review_future = std::async(std::launch::async, [&](){
    auto user_review_client_wrapper = _user_review_client_pool->Pop();
    if (!user_review_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connected to user-review-service";
      throw se;
    }
    auto user_review_client = user_review_client_wrapper->GetClient();
    try {
      user_review_client->UploadUserReview(req_id, new_review.user_id,
          new_review.review_id, new_review.timestamp, writer_text_map);
    } catch (...) {
      _user_review_client_pool->Remove(user_review_client_wrapper);
      LOG(error) << "Failed to upload review to user-review-service";
      throw;
    }
    _user_review_client_pool->Push(user_review_client_wrapper);
  // });

  // movie_review_future = std::async(std::launch::async, [&](){
    auto movie_review_client_wrapper = _movie_review_client_pool->Pop();
    if (!movie_review_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connected to movie-review-service";
      throw se;
    }
    auto movie_review_client = movie_review_client_wrapper->GetClient();
    try {
      movie_review_client->UploadMovieReview(req_id, new_review.movie_id,
          new_review.review_id, new_review.timestamp, writer_text_map);
    } catch (...) {
      _movie_review_client_pool->Remove(movie_review_client_wrapper);
      LOG(error) << "Failed to upload review to movie-review-service";
      throw;
    }
    _movie_review_client_pool->Push(movie_review_client_wrapper);
  // });
  
  // try {
  //   review_future.get();
  //   user_review_future.get();
  //   movie_review_future.get();
  // } catch (...) {
  //   throw;
  // }
}

void ComposeReviewHandler::UploadMovieId(
    int64_t req_id,
    const std::string &movie_id,
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

  std::string key_counter = std::to_string(req_id) + ":counter";
  auto mc_client = _mc_client_pool->Pop();
  if (!mc_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  bool stored, found;
  uint32_t flags;
  // Initialize the counter to 0 if there it is not in the memcached
  bool success = mc_client->Add(key_counter, "0", 0, MMC_EXP_TIME, &stored);

  // error if it cannot be stored
  if (!success) {
    _mc_client_pool->Push(mc_client);
    LOG(error) << "Failed to initilize the counter for request " << req_id;
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to initilize the counter for request " + std::to_string(req_id);
    throw se;
  }

  // Store movie_id to memcached
  uint64_t counter_value;
  std::string key_movie_id = std::to_string(req_id) + ":movie_id";
  success = mc_client->Add(key_movie_id, movie_id, 0, MMC_EXP_TIME, &stored);
  if (!success) {
    _mc_client_pool->Push(mc_client);
    LOG(error) << "Cannot store movie_id of request " << req_id;
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Cannot store movie_id of request " + std::to_string(req_id);
    throw se;
  } else if (!stored) {
    // Another thread has uploaded movie_id, which is an unexpected behaviour.
    LOG(warning) << "movie_id of request " << req_id
                 << " has already been stored";
    std::string counter_value_str;
    success = mc_client->Get(key_counter, &found, &counter_value_str, &flags);
    if (!success || !found) {
      _mc_client_pool->Push(mc_client);
      LOG(error) << "Cannot get the counter of request " << req_id;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message = "Cannot get the counter of request " + std::to_string(req_id);
      throw se;
    }
    counter_value = std::stoul(counter_value_str);
  } else {
    // Atomically increment and get the counter value
    success = mc_client->Incr(key_counter, 1, &counter_value);
    if (!success) {
      _mc_client_pool->Push(mc_client);
      LOG(error) << "Cannot increment and get the counter of request "
          << req_id;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message =  "Cannot increment and get the counter of request " + std::to_string(req_id);
      throw se;
    }
  }
  LOG(debug) << "req_id " << req_id
      << " caching movie_id to Memcached finished";
  _mc_client_pool->Push(mc_client);

  // If this thread is the last one uploading the review components,
  // it is in charge of compose the request and upload to the microservices in
  // the next tier.
  if (counter_value == NUM_COMPONENTS) {
    _ComposeAndUpload(req_id, writer_text_map);
  }
  span->Finish();
}

void ComposeReviewHandler::UploadUserId(
    int64_t req_id, int64_t user_id,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadUserId",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::string key_counter = std::to_string(req_id) + ":counter";
  auto mc_client = _mc_client_pool->Pop();
  if (!mc_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  bool stored, found;
  uint32_t flags;
  // Initialize the counter to 0 if there it is not in the memcached
  bool success = mc_client->Add(key_counter, "0", 0, MMC_EXP_TIME, &stored);

  // error if it cannot be stored
  if (!success) {
    _mc_client_pool->Push(mc_client);
    LOG(error) << "Failed to initilize the counter for request " << req_id;
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to initilize the counter for request " + std::to_string(req_id);
    throw se;
  }

  // Store user_id to memcached
  uint64_t counter_value;
  std::string key_user_id = std::to_string(req_id) + ":user_id";
  std::string user_id_str = std::to_string(user_id);
  success = mc_client->Add(key_user_id, user_id_str, 0, MMC_EXP_TIME, &stored);
  if (!success) {
    _mc_client_pool->Push(mc_client);
    LOG(error) << "Cannot store user_id of request " << req_id;
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Cannot store user_id of request " + std::to_string(req_id);
    throw se;
  } else if (!stored) {
    // Another thread has uploaded user_id, which is an unexpected behaviour.
    LOG(warning) << "user_id of request " << req_id
                 << " has already been stored";
    std::string counter_value_str;
    success = mc_client->Get(key_counter, &found, &counter_value_str, &flags);
    if (!success || !found) {
      _mc_client_pool->Push(mc_client);
      LOG(error) << "Cannot get the counter of request " << req_id;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message = "Cannot get the counter of request " + std::to_string(req_id);
      throw se;
    }
    counter_value = std::stoul(counter_value_str);
  } else {
    // Atomically increment and get the counter value
    success = mc_client->Incr(key_counter, 1, &counter_value);
    if (!success) {
      _mc_client_pool->Push(mc_client);
      LOG(error) << "Cannot increment and get the counter of request "
          << req_id;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message =  "Cannot increment and get the counter of request " + std::to_string(req_id);
      throw se;
    }
  }
  LOG(debug) << "req_id " << req_id << "caching user to Memcached finished";
  _mc_client_pool->Push(mc_client);

  // If this thread is the last one uploading the review components,
  // it is in charge of compose the request and upload to the microservices in
  // the next tier.
  if (counter_value == NUM_COMPONENTS) {
    _ComposeAndUpload(req_id, writer_text_map);
  }
  span->Finish();
}

void ComposeReviewHandler::UploadUniqueId(
    int64_t req_id, int64_t review_id,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadUniqueId",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::string key_counter = std::to_string(req_id) + ":counter";
  auto mc_client = _mc_client_pool->Pop();
  if (!mc_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  bool stored, found;
  uint32_t flags;
  // Initialize the counter to 0 if there it is not in the memcached
  bool success = mc_client->Add(key_counter, "0", 0, MMC_EXP_TIME, &stored);

  // error if it cannot be stored
  if (!success) {
    _mc_client_pool->Push(mc_client);
    LOG(error) << "Failed to initilize the counter for request " << req_id;
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to initilize the counter for request " + std::to_string(req_id);
    throw se;
  }

  // Store review_id to memcached
  uint64_t counter_value;
  std::string key_unique_id = std::to_string(req_id) + ":review_id";
  std::string unique_id_str = std::to_string(review_id);
  success = mc_client->Add(key_unique_id, unique_id_str, 0, MMC_EXP_TIME, &stored);
  if (!success) {
    _mc_client_pool->Push(mc_client);
    LOG(error) << "Cannot store review_id of request " << req_id;
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Cannot store review_id of request " + std::to_string(req_id);
    throw se;
  } else if (!stored) {
    // Another thread has uploaded review_id, which is an unexpected behaviour.
    LOG(warning) << "review_id of request " << req_id
                 << " has already been stored";
    std::string counter_value_str;
    success = mc_client->Get(key_counter, &found, &counter_value_str, &flags);
    if (!success || !found) {
      _mc_client_pool->Push(mc_client);
      LOG(error) << "Cannot get the counter of request " << req_id;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message = "Cannot get the counter of request " + std::to_string(req_id);
      throw se;
    }
    counter_value = std::stoul(counter_value_str);
  } else {
    // Atomically increment and get the counter value
    success = mc_client->Incr(key_counter, 1, &counter_value);
    if (!success) {
      _mc_client_pool->Push(mc_client);
      LOG(error) << "Cannot increment and get the counter of request "
          << req_id;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message =  "Cannot increment and get the counter of request " + std::to_string(req_id);
      throw se;
    }
  }
  LOG(debug) << "req_id " << req_id
             << " caching review_id to Memcached finished";

  _mc_client_pool->Push(mc_client);

  // If this thread is the last one uploading the review components,
  // it is in charge of compose the request and upload to the microservices in
  // the next tier.
  if (counter_value == NUM_COMPONENTS) {
    _ComposeAndUpload(req_id, writer_text_map);
  }
  span->Finish();
}

void ComposeReviewHandler::UploadText(
    int64_t req_id,
    const std::string &text,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadText",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::string key_counter = std::to_string(req_id) + ":counter";
  auto mc_client = _mc_client_pool->Pop();
  if (!mc_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  bool stored, found;
  uint32_t flags;
  // Initialize the counter to 0 if there it is not in the memcached
  bool success = mc_client->Add(key_counter, "0", 0, MMC_EXP_TIME, &stored);

  // error if it cannot be stored
  if (!success) {
    _mc_client_pool->Push(mc_client);
    LOG(error) << "Failed to initilize the counter for request " << req_id;
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to initilize the counter for request " + std::to_string(req_id);
    throw se;
  }

  // Store text to memcached
  uint64_t counter_value;
  std::string key_text = std::to_string(req_id) + ":text";
  success = mc_client->Add(key_text, text, 0, MMC_EXP_TIME, &stored);
  if (!success) {
    _mc_client_pool->Push(mc_client);
    LOG(error) << "Cannot store text of request " << req_id;
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Cannot store text of request " + std::to_string(req_id);
    throw se;
  } else if (!stored) {
    // Another thread has uploaded text, which is an unexpected behaviour.
    LOG(warning) << "text of request " << req_id
                 << " has already been stored";
    std::string counter_value_str;
    success = mc_client->Get(key_counter, &found, &counter_value_str, &flags);
    if (!success || !found) {
      _mc_client_pool->Push(mc_client);
      LOG(error) << "Cannot get the counter of request " << req_id;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message = "Cannot get the counter of request " + std::to_string(req_id);
      throw se;
    }
    counter_value = std::stoul(counter_value_str);
  } else {
    // Atomically increment and get the counter value
    success = mc_client->Incr(key_counter, 1, &counter_value);
    if (!success) {
      _mc_client_pool->Push(mc_client);
      LOG(error) << "Cannot increment and get the counter of request "
          << req_id;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message =  "Cannot increment and get the counter of request " + std::to_string(req_id);
      throw se;
    }
  }
  LOG(debug) << "req_id " << req_id << "caching text to Memcached finished";
  _mc_client_pool->Push(mc_client);

  // If this thread is the last one uploading the review components,
  // it is in charge of compose the request and upload to the microservices in
  // the next tier.
  if (counter_value == NUM_COMPONENTS) {
    _ComposeAndUpload(req_id, writer_text_map);
  }
  span->Finish();
}

void ComposeReviewHandler::UploadRating(
    int64_t req_id, int32_t rating, const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadRating",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  std::string key_counter = std::to_string(req_id) + ":counter";
  auto mc_client = _mc_client_pool->Pop();
  if (!mc_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  bool stored, found;
  uint32_t flags;
  // Initialize the counter to 0 if there it is not in the memcached
  bool success = mc_client->Add(key_counter, "0", 0, MMC_EXP_TIME, &stored);

  // error if it cannot be stored
  if (!success) {
    _mc_client_pool->Push(mc_client);
    LOG(error) << "Failed to initilize the counter for request " << req_id;
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to initilize the counter for request " + std::to_string(req_id);
    throw se;
  }

  // Store rating to memcached
  uint64_t counter_value;
  std::string key_rating = std::to_string(req_id) + ":rating";
  std::string rating_str = std::to_string(rating);
  success = mc_client->Add(key_rating, rating_str, 0, MMC_EXP_TIME, &stored);
  if (!success) {
    _mc_client_pool->Push(mc_client);
    LOG(error) << "Cannot store rating of request " << req_id;
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Cannot store rating of request " + std::to_string(req_id);
    throw se;
  } else if (!stored) {
    // Another thread has uploaded rating, which is an unexpected behaviour.
    LOG(warning) << "rating of request " << req_id
                 << " has already been stored";
    std::string counter_value_str;
    success = mc_client->Get(key_counter, &found, &counter_value_str, &flags);
    if (!success || !found) {
      _mc_client_pool->Push(mc_client);
      LOG(error) << "Cannot get the counter of request " << req_id;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message = "Cannot get the counter of request " + std::to_string(req_id);
      throw se;
    }
    counter_value = std::stoul(counter_value_str);
  } else {
    // Atomically increment and get the counter value
    success = mc_client->Incr(key_counter, 1, &counter_value);
    if (!success) {
      _mc_client_pool->Push(mc_client);
      LOG(error) << "Cannot increment and get the counter of request "
          << req_id;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message =  "Cannot increment and get the counter of request " + std::to_string(req_id);
      throw se;
    }
  }
  LOG(debug) << "req_id " << req_id << " caching rating to Memcached finished";
  _mc_client_pool->Push(mc_client);

  // If this thread is the last one uploading the review components,
  // it is in charge of compose the request and upload to the microservices in
  // the next tier.
  if (counter_value == NUM_COMPONENTS) {
    _ComposeAndUpload(req_id, writer_text_map);
  }
  span->Finish();
}

} // namespace media_service


#endif //MEDIA_MICROSERVICES_COMPOSEREVIEWHANDLER_H
