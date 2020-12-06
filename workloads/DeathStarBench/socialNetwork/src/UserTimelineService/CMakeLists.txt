add_library(
    UserTimelineService SHARED
    UserTimelineService.cpp
    ${THRIFT_GEN_CPP_DIR}/UserTimelineService.cpp
    ${THRIFT_GEN_CPP_DIR}/social_network_types.cpp
    ${THRIFT_GEN_CPP_DIR}/PostStorageService.cpp
)

target_include_directories(
    UserTimelineService PRIVATE
    ${THRIFT_INCLUDE_DIRS}
    ${MONGOC_INCLUDE_DIRS}
    /usr/local/include/jaegertracing
)

target_link_libraries(
    UserTimelineService
    ${MONGOC_LIBRARIES}
    nlohmann_json::nlohmann_json
    thrift_static
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    Boost::log
    Boost::log_setup
    jaegertracing
    /usr/local/lib/libhiredis.a
)

install(TARGETS UserTimelineService DESTINATION ./)