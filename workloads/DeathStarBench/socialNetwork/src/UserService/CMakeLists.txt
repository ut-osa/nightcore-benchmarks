add_library(
    UserService SHARED
    UserService.cpp
    ${THRIFT_GEN_CPP_DIR}/UserService.cpp
    ${THRIFT_GEN_CPP_DIR}/ComposePostService.cpp
    ${THRIFT_GEN_CPP_DIR}/social_network_types.cpp
    ${THRIFT_GEN_CPP_DIR}/SocialGraphService.cpp
)

target_include_directories(
    UserService PRIVATE
    ${THRIFT_INCLUDE_DIRS}
    ${LIBMEMCACHED_INCLUDE_DIR}
    ${MONGOC_INCLUDE_DIRS}
    /usr/local/include/jwt
    /usr/local/include/jaegertracing
)

target_link_libraries(
    UserService
    ${MONGOC_LIBRARIES}
    mc
    nlohmann_json::nlohmann_json
    thrift_static
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    Boost::log
    Boost::log_setup
    jaegertracing
    OpenSSL::SSL
)

install(TARGETS UserService DESTINATION ./)