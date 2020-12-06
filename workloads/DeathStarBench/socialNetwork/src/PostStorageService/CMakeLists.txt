add_library(
    PostStorageService SHARED
    PostStorageService.cpp
    ${THRIFT_GEN_CPP_DIR}/PostStorageService.cpp
    ${THRIFT_GEN_CPP_DIR}/social_network_types.cpp
)

target_include_directories(
    PostStorageService PRIVATE
    ${THRIFT_INCLUDE_DIRS}
    ${LIBMEMCACHED_INCLUDE_DIR}
    ${MONGOC_INCLUDE_DIRS}
    /usr/local/include/jaegertracing
)

target_link_libraries(
    PostStorageService
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

install(TARGETS PostStorageService DESTINATION ./)