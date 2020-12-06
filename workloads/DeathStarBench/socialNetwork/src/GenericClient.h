#ifndef SOCIAL_NETWORK_MICROSERVICES_GENERICCLIENT_H
#define SOCIAL_NETWORK_MICROSERVICES_GENERICCLIENT_H

#include <string>

namespace social_network {

class GenericClient{
 public:
  virtual ~GenericClient() = default;
  virtual void Connect() = 0;
  virtual void KeepAlive() = 0;
  virtual void KeepAlive(int) = 0;
  virtual void Disconnect() = 0;
  virtual bool IsConnected() = 0;
  uint16_t GetClientId() { return _client_id; }

 protected:
  std::string _addr;
  int _port;
  uint16_t _client_id;
};

} // namespace social_network

#endif //SOCIAL_NETWORK_MICROSERVICES_GENERICCLIENT_H
