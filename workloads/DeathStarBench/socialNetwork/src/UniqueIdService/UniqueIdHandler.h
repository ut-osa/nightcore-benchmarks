#ifndef SOCIAL_NETWORK_MICROSERVICES_UNIQUEIDHANDLER_H
#define SOCIAL_NETWORK_MICROSERVICES_UNIQUEIDHANDLER_H

#include <iostream>
#include <string>
#include <chrono>
#include <mutex>
#include <sstream>
#include <iomanip>
#include <arpa/inet.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/socket.h>


#include "../../gen-cpp/UniqueIdService.h"
#include "../../gen-cpp/ComposePostService.h"
#include "../../gen-cpp/social_network_types.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"

// Custom Epoch (January 1, 2018 Midnight GMT = 2018-01-01T00:00:00Z)
#define CUSTOM_EPOCH 1514764800000

namespace social_network {

using std::chrono::milliseconds;
using std::chrono::duration_cast;
using std::chrono::system_clock;

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

class UniqueIdHandler : public UniqueIdServiceIf {
 public:
  ~UniqueIdHandler() override = default;
  UniqueIdHandler(
      std::mutex *,
      const std::string &,
      ClientPool<ThriftClient<ComposePostServiceClient>> *);

  void UploadUniqueId(int64_t, PostType::type,
      const std::map<std::string, std::string> &) override;

 private:
  std::mutex *_thread_lock;
  std::string _machine_id;
  ClientPool<ThriftClient<ComposePostServiceClient>> *_compose_client_pool;
};

UniqueIdHandler::UniqueIdHandler(
    std::mutex *thread_lock,
    const std::string &machine_id,
    ClientPool<ThriftClient<ComposePostServiceClient>> *compose_client_pool) {
  LOG(info) << "machine_id=" << machine_id;
  _thread_lock = thread_lock;
  _machine_id = machine_id;
  _compose_client_pool = compose_client_pool;
}

void UniqueIdHandler::UploadUniqueId(
    int64_t req_id,
    PostType::type post_type,
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

  _thread_lock->lock();
  int64_t timestamp = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()).count() - CUSTOM_EPOCH;
  int idx = GetCounter(timestamp);
  _thread_lock->unlock();

  std::stringstream sstream;
  sstream << std::hex << timestamp;
  std::string timestamp_hex(sstream.str());

  if (timestamp_hex.size() > 10) {
    timestamp_hex.erase(0, timestamp_hex.size() - 10);
  } else if (timestamp_hex.size() < 10) {
    timestamp_hex = std::string(10 - timestamp_hex.size(), '0') + timestamp_hex;
  }

  // Empty the sstream buffer.
  sstream.clear();
  sstream.str(std::string());

  sstream << std::hex << idx;
  std::string counter_hex(sstream.str());

  if (counter_hex.size() > 3) {
    counter_hex.erase(0, counter_hex.size() - 3);
  } else if (counter_hex.size() < 3) {
    counter_hex = std::string(3 - counter_hex.size(), '0') + counter_hex;
  }
  std::string post_id_str = _machine_id + timestamp_hex + counter_hex;
  int64_t post_id = stoul(post_id_str, nullptr, 16) & 0x7FFFFFFFFFFFFFFF;
  LOG(debug) << "The post_id of the request "
      << req_id << " is " << post_id;

  // Upload to compose post service
  auto compose_post_client_wrapper = _compose_client_pool->Pop();
  if (!compose_post_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to compose-post-service";
    throw se;
  }
  auto compose_post_client = compose_post_client_wrapper->GetClient();
  {
    auto rpc_trace_guard = _compose_client_pool->StartRpcTrace("UploadUniqueId", compose_post_client_wrapper);
    try {
      compose_post_client->UploadUniqueId(req_id, post_id, post_type, writer_text_map);    
    } catch (...) {
      rpc_trace_guard->set_status(1);
      _compose_client_pool->Remove(compose_post_client_wrapper);
      LOG(error) << "Failed to upload unique-id to compose-post-service";
      throw;
    }
  }
  _compose_client_pool->Push(compose_post_client_wrapper);

  span->Finish();
}

/*
 * The following code which obtaines machine ID from machine's MAC address was
 * inspired from https://stackoverflow.com/a/16859693.
 */
u_int16_t HashMacAddressPid(const std::string &mac)
{
  u_int16_t hash = 0;
  srand(getpid());
  std::string mac_pid = mac + std::to_string(rand() + rand() + rand());
  for ( unsigned int i = 0; i < mac_pid.size(); i++ ) {
    hash += ( (u_int16_t)mac_pid[i] << (( i & 1 ) * 8 ));
  }
  return hash;
}

int GetMachineId (std::string *mac_hash) {
  std::string mac;

  std::ifstream hostname_file("/proc/sys/kernel/hostname");
  std::getline(hostname_file, mac);

  std::stringstream stream;
  stream << std::hex << HashMacAddressPid(mac);
  *mac_hash = stream.str();

  if (mac_hash->size() > 3) {
    mac_hash->erase(0, mac_hash->size() - 3);
  } else if (mac_hash->size() < 3) {
    *mac_hash = std::string(3 - mac_hash->size(), '0') + *mac_hash;
  }
  LOG(info) << "mac_hash=" << *mac_hash;
  return 0;
}

} // namespace social_network

#endif //SOCIAL_NETWORK_MICROSERVICES_UNIQUEIDHANDLER_H
