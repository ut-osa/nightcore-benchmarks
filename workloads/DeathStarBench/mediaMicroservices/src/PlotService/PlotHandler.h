#ifndef MEDIA_MICROSERVICES_PLOTHANDLER_H
#define MEDIA_MICROSERVICES_PLOTHANDLER_H

#include <iostream>
#include <string>

#include <mongoc.h>
#include <bson/bson.h>

#include "../../gen-cpp/PlotService.h"
#include "../logger.h"
#include "../tracing.h"
#include "../ClientPool.h"

namespace media_service {

class PlotHandler : public PlotServiceIf {
 public:
  PlotHandler(
      ClientPool<MCClient> *,
      mongoc_client_pool_t *);
  ~PlotHandler() override = default;

  void WritePlot(int64_t req_id, int64_t plot_id, const std::string& plot,
      const std::map<std::string, std::string> & carrier) override;
  void ReadPlot(std::string& _return, int64_t req_id, int64_t plot_id,
      const std::map<std::string, std::string> & carrier) override;

 private:
  ClientPool<MCClient> *_mc_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
};

PlotHandler::PlotHandler(
    ClientPool<MCClient> *mc_client_pool,
    mongoc_client_pool_t *mongodb_client_pool) {
  _mc_client_pool = mc_client_pool;
  _mongodb_client_pool = mongodb_client_pool;
}

void PlotHandler::ReadPlot(
    std::string &_return,
    int64_t req_id,
    int64_t plot_id,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "ReadPlot",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  auto mc_client = _mc_client_pool->Pop();
  if (!mc_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  uint32_t memcached_flags;

  // Look for the movie id from memcached
  auto get_span = opentracing::Tracer::Global()->StartSpan(
      "MmcGetPlot", { opentracing::ChildOf(&span->context()) });
  auto plot_id_str = std::to_string(plot_id);

  std::string plot_mmc;
  bool found;
  bool success = mc_client->Get(plot_id_str, &found, &plot_mmc, &memcached_flags);
  if (!success) {
    _mc_client_pool->Push(mc_client);
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to get plot from memcached";
    throw se;
  }
  get_span->Finish();
  _mc_client_pool->Push(mc_client);

  // If cached in memcached
  if (found) {
    LOG(debug) << "Get plot " << plot_mmc
        << " cache hit from Memcached";
    _return = std::string(plot_mmc);
  } else {
    // If not cached in memcached
    mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
        _mongodb_client_pool);
    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }
    auto collection = mongoc_client_get_collection(
        mongodb_client, "plot", "plot");
    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection plot from DB plot";
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }

    bson_t *query = bson_new();
    BSON_APPEND_INT64(query, "plot_id", plot_id);

    auto find_span = opentracing::Tracer::Global()->StartSpan(
        "MongoFindPlot", { opentracing::ChildOf(&span->context()) });
    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
        collection, query, nullptr, nullptr);
    const bson_t *doc;
    bool found = mongoc_cursor_next(cursor, &doc);
    find_span->Finish();

    if (found) {
      bson_iter_t iter;
      if (bson_iter_init_find(&iter, doc, "plot")) {
        char *plot_mongo_char = bson_iter_value(&iter)->value.v_utf8.str;
        size_t plot_mongo_len = bson_iter_value(&iter)->value.v_utf8.len;
        LOG(debug) << "Find plot " << plot_id << " cache miss";
        _return = std::string(plot_mongo_char, plot_mongo_char + plot_mongo_len);
        bson_destroy(query);
        mongoc_cursor_destroy(cursor);
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        auto mc_client = _mc_client_pool->Pop();
        if (!mc_client) {
          ServiceException se;
          se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
          se.message = "Failed to pop a client from memcached pool";
          throw se;
        }

        // Upload the plot to memcached
        auto set_span = opentracing::Tracer::Global()->StartSpan(
            "MmcSetPlot", { opentracing::ChildOf(&span->context()) });
        bool success = mc_client->Set(plot_id_str, _return, 0, 0);
        set_span->Finish();

        if (!success) {
          LOG(warning) << "Failed to set plot to Memcached";
        }
        _mc_client_pool->Push(mc_client);
      } else {
        LOG(error) << "Attribute plot is not find in MongoDB";
        bson_destroy(query);
        mongoc_cursor_destroy(cursor);
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        ServiceException se;
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message = "Attribute plot is not find in MongoDB";
        throw se;
      }
    } else {
      LOG(error) << "Plot_id " << plot_id << " is not found in MongoDB";
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
      se.message = "Plot_id " + plot_id_str + " is not found in MongoDB";
      throw se;
    }
  }
  span->Finish();
}

void PlotHandler::WritePlot(
    int64_t req_id,
    int64_t plot_id,
    const std::string &plot,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "WritePlot",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  bson_t *new_doc = bson_new();
  BSON_APPEND_INT64(new_doc, "plot_id", plot_id);
  BSON_APPEND_UTF8(new_doc, "plot", plot.c_str());

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
      _mongodb_client_pool);
  if (!mongodb_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to pop a client from MongoDB pool";
    throw se;
  }
  auto collection = mongoc_client_get_collection(
      mongodb_client, "plot", "plot");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection plot from DB plot";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }
  bson_error_t error;
  auto insert_span = opentracing::Tracer::Global()->StartSpan(
      "MongoInsertPlot", { opentracing::ChildOf(&span->context()) });
  bool plotinsert = mongoc_collection_insert_one (
      collection, new_doc, nullptr, nullptr, &error);
  insert_span->Finish();
  if (!plotinsert) {
    LOG(error) << "Error: Failed to insert plot to MongoDB: "
               << error.message;
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

} // namespace media_service

#endif //MEDIA_MICROSERVICES_PLOTHANDLER_H
