#ifndef SOCIAL_NETWORK_MICROSERVICES_THRIFTCLIENT_H
#define SOCIAL_NETWORK_MICROSERVICES_THRIFTCLIENT_H

#include <string>
#include <thread>
#include <iostream>
#include <boost/log/trivial.hpp>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/THttpClient.h>
#include <thrift/stdcxx.h>
#include "logger.h"
#include "GenericClient.h"
#include "FaasWorker.h"

namespace social_network {

using apache::thrift::protocol::TProtocol;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::transport::TFramedTransport;
using apache::thrift::transport::THttpClient;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransport;
using apache::thrift::TException;

template<class TThriftClient>
class ThriftClient : public GenericClient {
 public:
  ThriftClient(const std::string &addr, int port, const std::string& http_path, FaasWorker* faas_worker, uint16_t client_id);

  ThriftClient(const ThriftClient &) = delete;
  ThriftClient &operator=(const ThriftClient &) = delete;
  ThriftClient(ThriftClient<TThriftClient> &&) = default;
  ThriftClient &operator=(ThriftClient &&) = default;

  ~ThriftClient() override;

  TThriftClient *GetClient() const;

  void Connect() override;
  void Disconnect() override;
  void KeepAlive() override;
  void KeepAlive(int timeout_ms) override;
  bool IsConnected() override;

 private:
  std::shared_ptr<TThriftClient> _client;
  std::string _http_path;

  std::shared_ptr<TTransport> _socket;
  std::shared_ptr<TTransport> _transport;
  std::shared_ptr<TProtocol> _protocol;
};

template<class TThriftClient>
ThriftClient<TThriftClient>::ThriftClient(
    const std::string &addr, int port, const std::string& http_path, FaasWorker* faas_worker, uint16_t client_id) {
  _addr = addr;
  _port = port;
  _http_path = http_path;
  _client_id = client_id;
  const char* force_normal_client = getenv("THRIFT_FORCE_NORMAL_CLIENT");
  if (faas_worker == nullptr || (force_normal_client != nullptr && atoi(force_normal_client) == 1)) {
    TSocket* socket = new TSocket(addr, port);
    const char* timeout_ms_str = getenv("THRIFT_CLIENT_TIMEOUT_MS");
    if (timeout_ms_str != nullptr) {
      int timeout_ms = atoi(timeout_ms_str);
      LOG(info) << "Set socket timeout to " << timeout_ms << "ms";
      socket->setConnTimeout(timeout_ms);
      socket->setRecvTimeout(timeout_ms);
      socket->setSendTimeout(timeout_ms);
    }
    if (faas_worker != nullptr) {
      _http_path = std::string("/function/") + _http_path;
    }
    LOG(info) << "Connect to " << _addr << ":" << _port << " " << _http_path;
    _socket = std::shared_ptr<TSocket>(socket);
    _transport = std::shared_ptr<TTransport>(new THttpClient(_socket, _addr + ":" + std::to_string(_port), _http_path));
    _protocol = std::shared_ptr<TProtocol>(new TBinaryProtocol(_transport));
    _client = std::shared_ptr<TThriftClient>(new TThriftClient(_protocol));
  } else {
    LOG(info) << "Use FaaSWorker for thrift calls";
    _client = faas_worker->CreateClient<TThriftClient>(_http_path);
  }
}

template<class TThriftClient>
ThriftClient<TThriftClient>::~ThriftClient() {
  Disconnect();
}

template<class TThriftClient>
TThriftClient *ThriftClient<TThriftClient>::GetClient() const {
  return _client.get();
}

template<class TThriftClient>
bool ThriftClient<TThriftClient>::IsConnected() {
  if (_transport == nullptr) return true;
  return _transport->isOpen();
}

template<class TThriftClient>
void ThriftClient<TThriftClient>::Connect() {
  if (_transport == nullptr) return;
  if (!IsConnected()) {
    try {
      _transport->open();
    } catch (TException &tx) {
      throw tx;
    }
  }
}

template<class TThriftClient>
void ThriftClient<TThriftClient>::Disconnect() {
  if (_transport == nullptr) return;
  if (IsConnected()) {
    try {
      _transport->close();
    } catch (TException &tx) {
      throw tx;
    }
  }
}

template<class TThriftClient>
void ThriftClient<TThriftClient>::KeepAlive() {

}

// TODO: Implement KeepAlive Timeout
template<class TThriftClient>
void ThriftClient<TThriftClient>::KeepAlive(int timeout_ms) {

}

} // namespace social_network


#endif //SOCIAL_NETWORK_MICROSERVICES_THRIFTCLIENT_H
