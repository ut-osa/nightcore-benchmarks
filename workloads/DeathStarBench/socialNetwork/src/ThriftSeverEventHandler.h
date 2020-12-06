#ifndef SOCIAL_NETWORK_MICROSERVICES_THRIFTSERVEREVENTHANDLER_H
#define SOCIAL_NETWORK_MICROSERVICES_THRIFTSERVEREVENTHANDLER_H

#include <mutex>
#include <map>
#include <memory>

#include <thrift/server/TServer.h>
#include "logger.h"

namespace social_network {

using apache::thrift::protocol::TProtocol;
using apache::thrift::transport::TTransport;
using apache::thrift::server::TServerEventHandler;

class ThriftServerEventHandler : public TServerEventHandler {
 public:
  explicit ThriftServerEventHandler(const std::string& service_name) : _service_name(service_name) {}

  void preServe() override {
    LOG(info) << "[" << _service_name << "]: start serving";
  }

  void* createContext(std::shared_ptr<TProtocol> input,
                      std::shared_ptr<TProtocol> output) override {
    std::lock_guard<std::mutex> lk(_mu);
    ServerContext* context = new ServerContext;
    context->id = _current_context_id++;
    _server_contexts[context] = std::unique_ptr<ServerContext>(context);
    LOG(info) << "[" << _service_name << "]: create context with id " << context->id;
    LOG(info) << "[" << _service_name << "]: num_context=" << _server_contexts.size();
    return context;
  }

  void deleteContext(void* serverContext,
                     std::shared_ptr<TProtocol> input,
                     std::shared_ptr<TProtocol> output) override {
    std::lock_guard<std::mutex> lk(_mu);
    ServerContext* context = static_cast<ServerContext*>(serverContext);
    if (_server_contexts.count(context) == 0) {
      LOG(error) << "[" << _service_name << "]: delete unknown context!";
      return;
    }
    LOG(info) << "[" << _service_name << "]: delete context with id " << context->id;
    _server_contexts.erase(context);
    LOG(info) << "[" << _service_name << "]: num_context=" << _server_contexts.size();
  } 

  void processContext(void* serverContext, std::shared_ptr<TTransport> transport) override {
    ServerContext* context = static_cast<ServerContext*>(serverContext);
    LOG(debug) << "[" << _service_name << "]: process context with id " << context->id;
  }

 private:
  struct ServerContext {
    int id;
  };

  std::string _service_name;

  std::mutex _mu;
  int _current_context_id = 0;
  std::map<ServerContext*, std::unique_ptr<ServerContext>> _server_contexts;
};

}

#endif
