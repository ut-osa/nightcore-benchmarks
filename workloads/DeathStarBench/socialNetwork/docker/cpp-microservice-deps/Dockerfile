FROM ubuntu:18.04

ARG LIB_MONGOC_VERSION=1.16.1
ARG LIB_THRIFT_VERSION=0.12.0
ARG LIB_JSON_VERSION=3.6.1
ARG LIB_JAEGER_VERSION=0.4.2
ARG LIB_YAML_VERSION=0.6.2
ARG LIB_OPENTRACING_VERSION=1.5.1
ARG LIB_CPP_JWT_VERSION=1.1.1
ARG LIB_HIREDIS_VERSION=0.14.0
ARG LIB_AMQP_CPP_VERSION=4.1.4
ARG LIB_SIMPLEAMQPCLIENT_VERSION=2.4.0
ARG LIB_ZMQ_VERSION=4.2.5

ARG NUM_CPUS=1
ARG BUILD_DEPS="ca-certificates g++ cmake wget git automake bison flex libboost-all-dev libevent-dev libssl-dev libtool make pkg-config librabbitmq-dev"

RUN apt-get update \
  && apt-get install -y ${BUILD_DEPS} --no-install-recommends

# Install libzmq
RUN cd /tmp \
  && git clone https://github.com/zeromq/libzmq.git \
  && cd libzmq && git checkout tags/v${LIB_ZMQ_VERSION} \
  && mkdir -p cmake-build \
  && cd cmake-build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DWITH_PERF_TOOL=OFF -DBUILD_TESTS=OFF .. \
  && make -j${NUM_CPUS} \
  && make install

# Install mongo-c-driver
RUN cd /tmp \
  && wget https://github.com/mongodb/mongo-c-driver/releases/download/${LIB_MONGOC_VERSION}/mongo-c-driver-${LIB_MONGOC_VERSION}.tar.gz \
  && tar -zxf mongo-c-driver-${LIB_MONGOC_VERSION}.tar.gz \
  && cd mongo-c-driver-${LIB_MONGOC_VERSION} \
  && mkdir -p cmake-build \
  && cd cmake-build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_TESTS=0 -DENABLE_EXAMPLES=0 .. \
  && make -j${NUM_CPUS} \
  && make install

# Install /nlohmann/json
RUN cd /tmp \
  && wget -O json-${LIB_JSON_VERSION}.tar.gz https://github.com/nlohmann/json/archive/v${LIB_JSON_VERSION}.tar.gz \
  && tar -zxf json-${LIB_JSON_VERSION}.tar.gz \
  && cd json-${LIB_JSON_VERSION} \
  && mkdir -p cmake-build \
  && cd cmake-build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=0 .. \
  && make -j${NUM_CPUS} \
  && make install

# Install yaml-cpp
RUN cd /tmp \
  && wget -O yaml-cpp-${LIB_YAML_VERSION}.tar.gz https://github.com/jbeder/yaml-cpp/archive/yaml-cpp-${LIB_YAML_VERSION}.tar.gz \
  && tar -zxf yaml-cpp-${LIB_YAML_VERSION}.tar.gz \
  && cd yaml-cpp-yaml-cpp-${LIB_YAML_VERSION} \
  && mkdir -p cmake-build \
  && cd cmake-build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-fPIC" -DYAML_CPP_BUILD_TESTS=0 .. \
  && make -j${NUM_CPUS} \
  && make install

# Install opentracing-cpp
RUN cd /tmp \
  && wget -O opentracing-cpp-${LIB_OPENTRACING_VERSION}.tar.gz https://github.com/opentracing/opentracing-cpp/archive/v${LIB_OPENTRACING_VERSION}.tar.gz \
  && tar -zxf opentracing-cpp-${LIB_OPENTRACING_VERSION}.tar.gz \
  && cd opentracing-cpp-${LIB_OPENTRACING_VERSION} \
  && mkdir -p cmake-build \
  && cd cmake-build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-fPIC" -DBUILD_TESTING=0 .. \
  && make -j${NUM_CPUS} \
  && make install

# Install jwt
RUN cd /tmp \
  && wget -O cpp-jwt-${LIB_CPP_JWT_VERSION}.tar.gz https://github.com/arun11299/cpp-jwt/archive/v${LIB_CPP_JWT_VERSION}.tar.gz \
  && tar -zxf cpp-jwt-${LIB_CPP_JWT_VERSION}.tar.gz \
  && cd cpp-jwt-${LIB_CPP_JWT_VERSION} \
  && cp -R include/jwt /usr/local/include \
  # use the dependency in /usr/local/include instead of in jwt/json
  && rm -rf /usr/local/include/jwt/json \
  && sed -i 's/\#include \"jwt\/json\/json.hpp\"/\#include \<nlohmann\/json\.hpp\>/g' /usr/local/include/jwt/jwt.hpp

# Install hiredis
RUN cd /tmp \
  && git clone https://github.com/redis/hiredis.git \
  && cd hiredis && git checkout tags/v${LIB_HIREDIS_VERSION} \
  && make -j${NUM_CPUS} \
  && make install

# Install AMQP-CPP
RUN cd /tmp \
  && git clone https://github.com/CopernicaMarketingSoftware/AMQP-CPP.git \
  && cd AMQP-CPP && git checkout v${LIB_AMQP_CPP_VERSION} \
  && mkdir cmake-build && cd cmake-build \
  && cmake .. -DCMAKE_BUILD_TYPE=Release -DAMQP-CPP_BUILD_SHARED=on -DAMQP-CPP_LINUX_TCP=on \
  && make -j${NUM_CPUS} && make install

# Install SimpleAmqpClient
RUN cd /tmp \
  && git clone https://github.com/alanxz/SimpleAmqpClient.git \
  && cd SimpleAmqpClient \
  && git checkout v${LIB_SIMPLEAMQPCLIENT_VERSION} \
  && mkdir cmake-build && cd cmake-build \
  && cmake .. -DCMAKE_BUILD_TYPE=Release \
  && make -j${NUM_CPUS} \
  && make install

# Install lib-thrift
RUN cd /tmp \
  && wget -O thrift-${LIB_THRIFT_VERSION}.tar.gz https://github.com/apache/thrift/archive/v${LIB_THRIFT_VERSION}.tar.gz \
  && tar -zxf thrift-${LIB_THRIFT_VERSION}.tar.gz \
  && cd thrift-${LIB_THRIFT_VERSION} \
  && mkdir -p cmake-build \
  && cd cmake-build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=0 -DWITH_SHARED_LIB=0 -DCMAKE_CXX_FLAGS="-fPIC" .. \
  && make -j${NUM_CPUS} \
  && make install

# Install jaeger-client-cpp
RUN cd /tmp \
  && wget -O jaeger-client-cpp-${LIB_JAEGER_VERSION}.tar.gz https://github.com/jaegertracing/jaeger-client-cpp/archive/v${LIB_JAEGER_VERSION}.tar.gz \
  && tar -zxf jaeger-client-cpp-${LIB_JAEGER_VERSION}.tar.gz \
  && cd jaeger-client-cpp-${LIB_JAEGER_VERSION} \
  && mkdir -p cmake-build \
  && cd cmake-build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-fPIC" -DHUNTER_ENABLED=0 -DBUILD_TESTING=0 -DJAEGERTRACING_WITH_YAML_CPP=1 -DJAEGERTRACING_BUILD_EXAMPLES=0 .. \
  && make -j${NUM_CPUS} \
  && make install

RUN cd /tmp \
  && rm -rf \
    mongo-c-driver-${LIB_MONGOC_VERSION}.tar.gz \
    mongo-c-driver-${LIB_MONGOC_VERSION} \
    thrift-${LIB_THRIFT_VERSION}.tar.gz \
    thrift-${LIB_THRIFT_VERSION} \
    json-${LIB_JSON_VERSION}.tar.gz \
    json-${LIB_JSON_VERSION} \
    jaeger-client-cpp-${LIB_JAEGER_VERSION}.tar.gz \
    jaeger-client-cpp-${LIB_JAEGER_VERSION} \
    yaml-cpp-${LIB_YAML_VERSION}.tar.gz \
    yaml-cpp-yaml-cpp-${LIB_YAML_VERSION} \
    opentracing-cpp-${LIB_OPENTRACING_VERSION}.tar.gz \
    opentracing-cpp-${LIB_OPENTRACING_VERSION} \
    cpp-jwt-${LIB_CPP_JWT_VERSION}.tar.gz \
    cpp-jwt-${LIB_CPP_JWT_VERSION} \
    cpp_redis \
    AMQP-CPP \
    SimpleAmqpClient

RUN rm -rf /usr/local/include/thrift \
    /usr/local/lib/libthrift.a \
    /usr/local/lib/libthriftnb.a \
    /usr/local/lib/libthriftz.a

ENV LD_LIBRARY_PATH /usr/local/lib:${LD_LIBRARY_PATH}
RUN ldconfig
