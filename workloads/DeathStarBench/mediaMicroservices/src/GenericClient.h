#ifndef MEDIA_MICROSERVICES_GENERICCLIENT_H
#define MEDIA_MICROSERVICES_GENERICCLIENT_H

#include <string>

namespace media_service {

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

} // namespace media_service

#endif //MEDIA_MICROSERVICES_GENERICCLIENT_H
