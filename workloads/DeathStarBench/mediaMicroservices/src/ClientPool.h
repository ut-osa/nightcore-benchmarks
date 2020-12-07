#ifndef SOCIAL_NETWORK_MICROSERVICES_CLIENTPOOL_H
#define SOCIAL_NETWORK_MICROSERVICES_CLIENTPOOL_H

#include <vector>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <chrono>
#include <string>
#include <atomic>

#include <boost/filesystem.hpp>

#include "logger.h"
#include "FaasWorker.h"

namespace media_service {

template<class TClient>
class ClientPool {
 public:
  ClientPool(const std::string &client_type, const std::string &addr,
      int port, int min_size, int max_size, int timeout_ms,
      const std::string& service_http_path = "/", FaasWorker* faas_worker = nullptr,
      const std::string& src_service = "", const std::string& dst_service = "");
  ~ClientPool();

  ClientPool(const ClientPool&) = delete;
  ClientPool& operator=(const ClientPool&) = delete;
  ClientPool(ClientPool&&) = default;
  ClientPool& operator=(ClientPool&&) = default;

  TClient * Pop();
  void Push(TClient *);
  void Push(TClient *, int);
  void Remove(TClient *);

  struct RpcTrace {
    uint64_t start_timestamp;
    uint32_t duration;
    uint16_t client_id;
    uint16_t status;
  };
  static_assert(sizeof(RpcTrace) == 16, "Unexpected size of RpcTrace");

  class RpcTraceGuard {
   public:
    RpcTraceGuard(RpcTrace* rpc_trace, std::atomic<int64_t>* onfly_rpcs)
        : _rpc_trace(rpc_trace), _onfly_rpcs(onfly_rpcs) {
      if (rpc_trace != nullptr) {
        rpc_trace->start_timestamp = current_timestamp();
      }
    }

    ~RpcTraceGuard() {
      if (_rpc_trace != nullptr) {
        uint64_t duration = current_timestamp() - _rpc_trace->start_timestamp;
        _rpc_trace->duration = static_cast<uint32_t>(duration);
        _onfly_rpcs->fetch_add(-1);
      }
    }

    void set_status(uint16_t status) {
      if (_rpc_trace != nullptr) {
        _rpc_trace->status = status;
      }
    }

   private:
    uint64_t current_timestamp() {
      return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()).count();
    }

    RpcTrace* _rpc_trace;
    std::atomic<int64_t>* _onfly_rpcs;
  };

  std::unique_ptr<RpcTraceGuard> StartRpcTrace(const char* method_name, TClient* client) {
    if (!_enable_rpc_trace) {
      return std::make_unique<RpcTraceGuard>(nullptr, nullptr);
    }
    RpcTrace* rpc_trace;
    {
      std::lock_guard<std::mutex> lk(_trace_mu);
      int method_idx = -1;
      for (size_t i = 0; i < _method_names.size(); i++) {
        if (_method_names[i] == method_name) {
          method_idx = i;
          break;
        }
      }
      if (method_idx == -1) {
        method_idx = _method_names.size();
        _method_names.push_back(method_name);
        _traces.push_back(std::make_unique<RpcTraceBlocks>());
      }
      rpc_trace = _traces[method_idx]->NewRpcTrace();
      _onfly_rpcs.fetch_add(1);
    }
    rpc_trace->client_id = client->GetClientId();
    rpc_trace->status = 0;
    return std::make_unique<RpcTraceGuard>(rpc_trace, &_onfly_rpcs);
  }

 private:
  std::deque<TClient *> _pool;
  std::string _addr;
  std::string _client_type;
  int _port;
  int _min_pool_size{};
  int _max_pool_size{};
  int _curr_pool_size{};
  int _timeout_ms;
  std::mutex _mtx;
  std::condition_variable _cv;
  std::atomic<uint16_t> _current_client_id{0};

  std::string _service_http_path;
  FaasWorker* _faas_worker;

  std::string _src_service;
  std::string _dst_service;

  class RpcTraceBlocks {
   public:
    explicit RpcTraceBlocks(size_t block_size = 65536) : _block_size(block_size), _current(0) {
      _blocks.push_back(new RpcTrace[block_size]);
    }
    ~RpcTraceBlocks() {
      for (size_t i = 0; i < _blocks.size(); i++) {
        delete[] _blocks[i];
      }
    }

    RpcTrace* NewRpcTrace() {
      if (_current == _block_size) {
        _blocks.push_back(new RpcTrace[_block_size]);
        _current = 0;
      }
      RpcTrace* ret = _blocks.back() + _current;
      _current++;
      return ret;
    }

    void FlushRpcTrace(FILE* fout) {
      for (size_t i = 0; i < _blocks.size(); i++) {
        if (i + 1 == _blocks.size()) {
          if (_current > 0) {
            fwrite(_blocks[i], sizeof(RpcTrace), _current, fout);
          }
        } else {
          fwrite(_blocks[i], sizeof(RpcTrace), _block_size, fout);
        }
      }
    }

   private:
    size_t _block_size;
    size_t _current;
    std::vector<RpcTrace*> _blocks;
  };

  const char* GetEnv(const char* name, const char* default_value) {
    const char* value = getenv(name);
    if (value != nullptr && strlen(value) > 0) {
      return value;
    } else {
      return default_value;
    }
  }

  std::mutex _trace_mu;
  std::vector<std::unique_ptr<RpcTraceBlocks>> _traces;
  std::vector<std::string> _method_names;
  std::atomic<int64_t> _onfly_rpcs{0};

  bool _enable_rpc_trace;
};

template<class TClient>
ClientPool<TClient>::ClientPool(const std::string &client_type,
    const std::string &addr, int port, int min_pool_size,
    int max_pool_size, int timeout_ms, const std::string& service_http_path, FaasWorker* faas_worker,
    const std::string& src_service, const std::string& dst_service) {
  _addr = addr;
  _port = port;
  _min_pool_size = min_pool_size;
  _max_pool_size = max_pool_size;
  _timeout_ms = timeout_ms;
  _client_type = client_type;
  _service_http_path = service_http_path;
  _faas_worker = faas_worker;

  LOG(info) << "Max pool size: " << max_pool_size;

  for (int i = 0; i < min_pool_size; ++i) {
    TClient *client = new TClient(addr, port, _service_http_path, _faas_worker, _current_client_id.fetch_add(1));
    _pool.emplace_back(client);
  }
  _curr_pool_size = min_pool_size;

  _src_service = src_service;
  _dst_service = dst_service;

  const char* enable_rpc_trace_str = GetEnv("ENABLE_RPC_TRACE", "0");
  if (atoi(enable_rpc_trace_str) == 1) {
    _enable_rpc_trace = true;
  } else {
    _enable_rpc_trace = false;
  }
}

template<class TClient>
ClientPool<TClient>::~ClientPool() {
  while (!_pool.empty()) {
    delete _pool.front();
    _pool.pop_front();
  }
}

template<class TClient>
TClient * ClientPool<TClient>::Pop() {
  TClient * client = nullptr;
  std::unique_lock<std::mutex> cv_lock(_mtx); {
    while (_pool.size() == 0) {
      // Create a new a client if current pool size is less than
      // the max pool size.
      if (_curr_pool_size < _max_pool_size) {
        try {
          client = new TClient(_addr, _port, _service_http_path, _faas_worker, _current_client_id.fetch_add(1));
          _curr_pool_size++;
          LOG(info) << "New " << _client_type << " client, total_client=" << _curr_pool_size;
          break;
        } catch (...) {
          cv_lock.unlock();
          return nullptr;
        }
      } else {
        auto wait_time = std::chrono::system_clock::now() +
            std::chrono::milliseconds(_timeout_ms);
        bool wait_success = _cv.wait_until(cv_lock, wait_time,
            [this] { return _pool.size() > 0; });
        if (!wait_success) {
          LOG(warning) << "ClientPool pop timeout";
          cv_lock.unlock();
          return nullptr;
        }
      }
    }
    if (!client){
      client = _pool.front();
      _pool.pop_front();
    }

  } // cv_lock(_mtx)
  cv_lock.unlock();

  if (client) {
    try {
      client->Connect();
    } catch (...) {
      LOG(error) << "Failed to connect " + _client_type;
      _pool.push_back(client);
      throw;
    }    
  }
  return client;
}

template<class TClient>
void ClientPool<TClient>::Push(TClient *client) {
  std::unique_lock<std::mutex> cv_lock(_mtx);
  client->KeepAlive();
  _pool.push_back(client);
  cv_lock.unlock();
  _cv.notify_one();
}

template<class TClient>
void ClientPool<TClient>::Push(TClient *client, int timeout_ms) {
  std::unique_lock<std::mutex> cv_lock(_mtx);
  client->KeepAlive(timeout_ms);
  _pool.push_back(client);
  cv_lock.unlock();
  _cv.notify_one();
}

template<class TClient>
void ClientPool<TClient>::Remove(TClient *client) {
  std::unique_lock<std::mutex> lock(_mtx);
  delete client;
  _curr_pool_size--;
  LOG(info) << "Remove " << _client_type << " client, total_client=" << _curr_pool_size;
  lock.unlock();
}

} // namespace social_network


#endif //SOCIAL_NETWORK_MICROSERVICES_CLIENTPOOL_H