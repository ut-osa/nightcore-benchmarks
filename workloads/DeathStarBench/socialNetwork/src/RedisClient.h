#ifndef SOCIAL_NETWORK_MICROSERVICES_REDISCLIENT_H
#define SOCIAL_NETWORK_MICROSERVICES_REDISCLIENT_H

#include <string>
#include <hiredis/hiredis.h>

#include "../gen-cpp/social_network_types.h"
#include "logger.h"
#include "GenericClient.h"

class FaasWorker;

namespace social_network {

class RedisClient : public GenericClient {
 public:
  RedisClient(const std::string &addr, int port, const std::string& http_path, FaasWorker* faas_worker, uint16_t client_id);
  RedisClient(const RedisClient &) = delete;
  RedisClient & operator=(const RedisClient &) = delete;
  RedisClient(RedisClient &&) = default;
  RedisClient & operator=(RedisClient &&) = default;

  ~RedisClient() override ;

  class RedisReplyWrapper {
   public:
    explicit RedisReplyWrapper(redisReply* reply, bool need_free = true)
      : _reply(reply), _need_free(need_free) {}
    ~RedisReplyWrapper() { if (_need_free) freeReplyObject(_reply); }

    std::string as_string() const {
      if (_reply->type != REDIS_REPLY_STRING) {
        ServiceException se;
        se.errorCode = ErrorCode::SE_REDIS_ERROR;
        se.message = "Failed to retrieve message from Redis";
        throw se;
      }
      return std::string(_reply->str, _reply->len);
    }

    long long as_integer() const {
      if (_reply->type != REDIS_REPLY_INTEGER) {
        ServiceException se;
        se.errorCode = ErrorCode::SE_REDIS_ERROR;
        se.message = "Failed to retrieve message from Redis";
        throw se;
      }
      return _reply->integer;
    }
    
    std::vector<std::unique_ptr<RedisReplyWrapper>> as_array() {
      if (_reply->type != REDIS_REPLY_ARRAY) {
        ServiceException se;
        se.errorCode = ErrorCode::SE_REDIS_ERROR;
        se.message = "Failed to retrieve message from Redis";
        throw se;
      }
      std::vector<std::unique_ptr<RedisReplyWrapper>> ret;
      for (size_t i = 0; i < _reply->elements; i++) {
        ret.emplace_back(new RedisReplyWrapper(_reply->element[i], false));
      }
      return ret;
    }

    bool ok() const {
      if (_reply->type == REDIS_REPLY_ERROR) return false;
      if (_reply->type == REDIS_REPLY_STATUS
          && strncmp(_reply->str, "OK", 2) != 0) {
        return false;
      }
      return true;
    }

    void check_ok() const {
      if (!ok()) {
        ServiceException se;
        se.errorCode = ErrorCode::SE_REDIS_ERROR;
        se.message = "Failed to retrieve message from Redis";
        throw se;
      }
    }

   private:
    redisReply* _reply;
    bool _need_free;
  };

  class RedisContextWrapper {
   public:
    explicit RedisContextWrapper(redisContext* context) : _context(context) {}

    void AppendCommand(const char *format, ...) {
      va_list ap;
      va_start(ap, format);
      int ret = redisvAppendCommand(_context, format, ap);
      va_end(ap);
      if (ret != REDIS_OK) {
        ServiceException se;
        se.errorCode = ErrorCode::SE_REDIS_ERROR;
        se.message = "Failed to retrieve message from Redis";
        throw se;
      }
    }

    std::unique_ptr<RedisReplyWrapper> GetReply() {
      redisReply* reply = nullptr;
      if (redisGetReply(_context, (void**)&reply) != REDIS_OK) {
        if (_context->err == REDIS_ERR_IO) {
          LOG(error) << "IO failed: " << strerror(errno);
        } else if (_context->err) {
          LOG(error) << "Redis error: " << _context->errstr;
        }
        if (reply != nullptr) freeReplyObject(reply);
        ServiceException se;
        se.errorCode = ErrorCode::SE_REDIS_ERROR;
        se.message = "Failed to retrieve message from Redis";
        throw se;
      }
      return std::unique_ptr<RedisReplyWrapper>(new RedisReplyWrapper(reply));
    }

   private:
    redisContext* _context;
  };

  RedisContextWrapper GetClient() const;

  void Connect() override ;
  void Disconnect() override ;
  void KeepAlive() override ;
  void KeepAlive(int timeout_ms) override ;
  bool IsConnected() override ;

 private:
  redisContext* _client;
};

RedisClient::RedisClient(const std::string &addr, int port,
                         const std::string& http_path, FaasWorker* faas_worker, uint16_t client_id) {
  _addr = addr;
  _port = port;
  _client_id = client_id;
  _client = nullptr;
}

RedisClient::~RedisClient() {
  Disconnect();
  delete _client;
}

RedisClient::RedisContextWrapper RedisClient::GetClient() const {
  return RedisContextWrapper(_client);
}

void RedisClient::Connect() {
  if (!IsConnected()) {
    _client = redisConnect(_addr.c_str(), _port);
    if (_client == nullptr || _client->err) {
      LOG(error) << "Failed to connect " << _addr << ":" << _port;
      ServiceException se;
      se.errorCode = ErrorCode::SE_REDIS_ERROR;
      se.message = "Failed to connect";
      throw se;
    }
    if (redisEnableKeepAlive(_client) != REDIS_OK) {
      LOG(error) << "redisEnableKeepAlive failed";
    }
  }
}

void RedisClient::Disconnect() {
  if (IsConnected()) {
    redisFree(_client);
    _client = nullptr;
  }
}

bool RedisClient::IsConnected() {
  return _client != nullptr;
}

void RedisClient::KeepAlive() {

}

void RedisClient::KeepAlive(int timeout_ms) {

}

} // social_network

#endif //SOCIAL_NETWORK_MICROSERVICES_REDISCLIENT_H
