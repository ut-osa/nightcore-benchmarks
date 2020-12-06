add_library(
    UserMentionService SHARED
    UserMentionService.cpp
    ${THRIFT_GEN_CPP_DIR}/UserMentionService.cpp
    ${THRIFT_GEN_CPP_DIR}/ComposePostService.cpp
    ${THRIFT_GEN_CPP_DIR}/social_network_types.cpp
)

target_include_directories(
    UserMentionService PRIVATE
    ${THRIFT_INCLUDE_DIRS}
    ${LIBMEMCACHED_INCLUDE_DIR}
    ${MONGOC_INCLUDE_DIRS}
    /usr/local/include/jaegertracing
)

target_link_libraries(
    UserMentionService
    ${MONGOC_LIBRARIES}
    mc
    nlohmann_json::nlohmann_json
    thrift_static
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    Boost::log
    Boost::log_setup
    jaegertracing
)

install(TARGETS UserMentionService DESTINATION ./)