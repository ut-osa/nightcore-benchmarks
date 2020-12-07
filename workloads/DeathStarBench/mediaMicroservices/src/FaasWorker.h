#ifndef SOCIAL_NETWORK_MICROSERVICES_FAAS_WORKER_H
#define SOCIAL_NETWORK_MICROSERVICES_FAAS_WORKER_H

#include <memory>

#include "./faas/worker_v1_interface.h"

#include <thrift/TProcessor.h>
#include <thrift/transport/TVirtualTransport.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TTransportException.h>
#include <thrift/protocol/TBinaryProtocol.h>

class FaasWorker {
public:
    FaasWorker(void* caller_context,
               faas_invoke_func_fn_t invoke_func_fn,
               faas_append_output_fn_t append_output_fn)
        : caller_context_(caller_context),
          invoke_func_fn_(invoke_func_fn), append_output_fn_(append_output_fn) {
        in_transport_ = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        out_transport_.reset(new WorkerOutputTransport(this));
        in_protocol_factory_.reset(new apache::thrift::protocol::TBinaryProtocolFactoryT<
            apache::thrift::transport::TMemoryBuffer, apache::thrift::protocol::TNetworkBigEndian>());
        out_protocol_factory_.reset(new apache::thrift::protocol::TBinaryProtocolFactoryT<
            WorkerOutputTransport, apache::thrift::protocol::TNetworkBigEndian>());
        client_protocol_factory_.reset(new apache::thrift::protocol::TBinaryProtocolFactoryT<
            ClientTransport, apache::thrift::protocol::TNetworkBigEndian>());
    }

    void SetProcessor(std::shared_ptr<apache::thrift::TProcessor> processor) {
        processor_ = processor;
    }

    bool Process(const char* input, size_t input_length) {
        if (processor_ == nullptr) {
            fprintf(stderr, "Processor is not set!\n");
            return false;
        }
        in_transport_->resetBuffer(reinterpret_cast<uint8_t*>(const_cast<char*>(input)),
                                   static_cast<uint32_t>(input_length));
        try {
            if (!processor_->process(in_protocol_factory_->getProtocol(in_transport_),
                                     out_protocol_factory_->getProtocol(out_transport_),
                                     nullptr)) {
                return false;
            }
        } catch (const std::exception& x) {
            fprintf(stderr, "Failed to process request: %s\n", x.what());
            return false;
        }
        return true;
    }

    template<class ClientType>
    std::shared_ptr<ClientType> CreateClient(const std::string& func_name) {
        std::shared_ptr<apache::thrift::transport::TTransport> transport(
            new ClientTransport(this, func_name));
        return std::make_shared<ClientType>(client_protocol_factory_->getProtocol(transport));
    }

private:
    class WorkerOutputTransport : public apache::thrift::transport::TVirtualTransport<WorkerOutputTransport> {
    public:
        explicit WorkerOutputTransport(FaasWorker* parent) : parent_(parent) {}

        void write(const uint8_t* buf, uint32_t len) {
            parent_->append_output_fn_(
                parent_->caller_context_,
                reinterpret_cast<const char*>(buf), static_cast<size_t>(len));
        }

    private:
        FaasWorker* parent_;
    };

    class ClientTransport : public apache::thrift::transport::TVirtualTransport<ClientTransport> {
    public:
        ClientTransport(FaasWorker* parent, const std::string& func_name)
            : parent_(parent), func_name_(func_name) {}

        void write(const uint8_t* buf, uint32_t len) {
            out_buf_.write(buf, len);
        }

        uint32_t read(uint8_t* buf, uint32_t len) {
            return in_buf_.read(buf, len);
        }

        uint32_t readAll(uint8_t* buf, uint32_t len) {
            return in_buf_.readAll(buf, len);
        }

        void flush() override {
            uint8_t* data;
            uint32_t data_length;
            out_buf_.getBuffer(&data, &data_length);
            const char* output;
            size_t output_length;
            if (parent_->invoke_func_fn_(parent_->caller_context_, func_name_.c_str(),
                                         reinterpret_cast<const char*>(data),
                                         static_cast<size_t>(data_length),
                                         &output, &output_length) != 0) {
                throw apache::thrift::transport::TTransportException(
                    apache::thrift::transport::TTransportException::UNKNOWN, "invoke_func call failed");
            }
            out_buf_.resetBuffer();
            in_buf_.resetBuffer(reinterpret_cast<uint8_t*>(const_cast<char*>(output)),
                                static_cast<uint32_t>(output_length));
        }

    private:
        FaasWorker* parent_;
        std::string func_name_;
        apache::thrift::transport::TMemoryBuffer in_buf_;
        apache::thrift::transport::TMemoryBuffer out_buf_;
    };

    void* caller_context_;
    faas_invoke_func_fn_t invoke_func_fn_;
    faas_append_output_fn_t append_output_fn_;

    std::shared_ptr<apache::thrift::TProcessor> processor_;
    std::shared_ptr<apache::thrift::transport::TMemoryBuffer> in_transport_;
    std::shared_ptr<apache::thrift::transport::TTransport> out_transport_;
    std::shared_ptr<apache::thrift::protocol::TProtocolFactory> in_protocol_factory_;
    std::shared_ptr<apache::thrift::protocol::TProtocolFactory> out_protocol_factory_;
    std::shared_ptr<apache::thrift::protocol::TProtocolFactory> client_protocol_factory_;

    FaasWorker(const FaasWorker&) = delete;
    FaasWorker& operator=(const FaasWorker&) = delete;
};

#endif
