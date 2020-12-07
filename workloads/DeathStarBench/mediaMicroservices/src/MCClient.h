#ifndef MEDIA_MICROSERVICES_MCCLIENT_H
#define MEDIA_MICROSERVICES_MCCLIENT_H

#include <string>

#include "logger.h"
#include "GenericClient.h"
#include "../libmc/include/c_client.h"

namespace media_service {

class MCClient : public GenericClient {
 public:
  MCClient(const std::string &addr, int port, const std::string& http_path, FaasWorker* faas_worker, uint16_t client_id);
  MCClient(const MCClient &) = delete;
  MCClient & operator=(const MCClient &) = delete;
  MCClient(MCClient &&) = default;
  MCClient & operator=(MCClient &&) = default;

  ~MCClient() override ;

  void Connect() override ;
  void Disconnect() override ;
  void KeepAlive() override ;
  void KeepAlive(int timeout_ms) override ;
  bool IsConnected() override ;

  bool Get(const std::string& key, bool* found, std::string* value, uint32_t* flags);
  bool Set(const std::string& key, const std::string& value, uint32_t flags, int64_t exptime);
  bool Add(const std::string& key, const std::string& value, uint32_t flags, int64_t exptime, bool* stored);
  bool Incr(const std::string& key, uint64_t delta, uint64_t* value);
  bool Delete(const std::string& key);

 private:
  void* _client;
};

MCClient::MCClient(const std::string &addr, int port,
                   const std::string& http_path, FaasWorker* faas_worker, uint16_t client_id) {
  _addr = addr;
  _port = port;
  _client_id = client_id;
  _client = nullptr;
}

MCClient::~MCClient() {
  Disconnect();
}

void MCClient::Connect() {
  if (!IsConnected()) {
    _client = client_create();
    const char* host = _addr.c_str();
    uint32_t port = _port;
    client_init(_client, &host, &port, 1, nullptr, 0);
  }
}

void MCClient::Disconnect() {
  if (IsConnected()) {
    client_destroy(_client);
    _client = nullptr;
  }
}

bool MCClient::IsConnected() {
  return _client != nullptr;
}

void MCClient::KeepAlive() {

}

void MCClient::KeepAlive(int timeout_ms) {

}

bool MCClient::Get(const std::string& key, bool* found, std::string* value, uint32_t* flags) {
  const char* key_ptr = key.data();
  const size_t key_len = key.size();
  retrieval_result_t** results = nullptr;
  size_t n_results = 0;
  err_code_t err = client_get(_client, &key_ptr, &key_len, 1, &results, &n_results);
  bool ret = true;
  if (err != RET_OK) {
    ret = false;
  } else {
    if (n_results == 0) {
      *found = false;
    } else {
      *found = true;
      value->assign(results[0]->data_block, results[0]->bytes);
      *flags = results[0]->flags;
    }
  }
  client_destroy_retrieval_result(_client);
  return ret;
}

bool MCClient::Set(const std::string& key, const std::string& value, uint32_t flags, int64_t exptime) {
  const char* key_ptr = key.data();
  const size_t key_len = key.size();
  const char* value_ptr = value.data();
  const size_t value_len = value.size();
  message_result_t** results = nullptr;
  size_t n_results = 0;
  err_code_t err = client_set(_client, &key_ptr, &key_len, &flags, exptime, nullptr, false,
                              &value_ptr, &value_len, 1, &results, &n_results);
  bool ret = true;
  if (err != RET_OK || n_results == 0) {
    ret = false;
  } else {
    if (results[0]->type_ != MSG_STORED) ret = false;
  }
  client_destroy_message_result(_client);
  return ret;
}

bool MCClient::Add(const std::string& key, const std::string& value, uint32_t flags, int64_t exptime, bool* stored) {
  const char* key_ptr = key.data();
  const size_t key_len = key.size();
  const char* value_ptr = value.data();
  const size_t value_len = value.size();
  message_result_t** results = nullptr;
  size_t n_results = 0;
  err_code_t err = client_add(_client, &key_ptr, &key_len, &flags, exptime, nullptr, false,
                              &value_ptr, &value_len, 1, &results, &n_results);
  bool ret = true;
  if (err != RET_OK || n_results == 0) {
    ret = false;
  } else {
    if (results[0]->type_ == MSG_STORED) {
      *stored = true;
    } else if (results[0]->type_ == MSG_NOT_STORED) {
      *stored = false;
    } else {
      ret = false;
    }
  }
  client_destroy_message_result(_client);
  return ret;
}

bool MCClient::Incr(const std::string& key, uint64_t delta, uint64_t* value) {
  const char* key_ptr = key.data();
  const size_t key_len = key.size();
  unsigned_result_t* results = nullptr;
  size_t n_results = 0;
  err_code_t err = client_incr(_client, key_ptr, key_len, delta, false, &results, &n_results);
  bool ret = true;
  if (err != RET_OK || n_results == 0) {
    ret = false;
  } else {
    *value = results->value;
  }
  client_destroy_message_result(_client);
  return ret;
}

bool MCClient::Delete(const std::string& key) {
  const char* key_ptr = key.data();
  const size_t key_len = key.size();
  message_result_t** results = nullptr;
  size_t n_results = 0;
  err_code_t err = client_delete(_client, &key_ptr, &key_len, false, 1, &results, &n_results);
  bool ret = true;
  if (err != RET_OK || n_results == 0) {
    ret = false;
  } else if (results[0]->type_ != MSG_DELETED && results[0]->type_ != MSG_NOT_FOUND) {
    ret = false;
  }
  client_destroy_retrieval_result(_client);
  return ret;
}

} // media_service

#endif //MEDIA_MICROSERVICES_MCCLIENT_H
