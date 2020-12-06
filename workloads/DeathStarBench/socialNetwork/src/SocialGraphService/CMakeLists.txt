add_library(
    SocialGraphService SHARED
    SocialGraphService.cpp
    ${THRIFT_GEN_CPP_DIR}/SocialGraphService.cpp
    ${THRIFT_GEN_CPP_DIR}/UserService.cpp
    ${THRIFT_GEN_CPP_DIR}/social_network_types.cpp
)

target_include_directories(
    SocialGraphService PRIVATE
    ${THRIFT_INCLUDE_DIRS}
    ${MONGOC_INCLUDE_DIRS}
    /usr/local/include/jaegertracing
)

target_link_libraries(
    SocialGraphService
    ${MONGOC_LIBRARIES}
    thrift_static
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    nlohmann_json::nlohmann_json
    Boost::log
    Boost::log_setup
    OpenSSL::SSL
    /usr/local/lib/libjaegertracing.so
    /usr/local/lib/libhiredis.a
)

install(TARGETS SocialGraphService DESTINATION ./)