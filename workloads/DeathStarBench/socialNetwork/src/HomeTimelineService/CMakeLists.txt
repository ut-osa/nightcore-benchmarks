add_library(
    HomeTimelineService SHARED
    HomeTimelineService.cpp
    ${THRIFT_GEN_CPP_DIR}/HomeTimelineService.cpp
    ${THRIFT_GEN_CPP_DIR}/PostStorageService.cpp
    ${THRIFT_GEN_CPP_DIR}/social_network_types.cpp
)

target_include_directories(
    HomeTimelineService PRIVATE
    ${THRIFT_INCLUDE_DIRS}
    /usr/local/include/jaegertracing
)

target_link_libraries(
    HomeTimelineService
    nlohmann_json::nlohmann_json
    thrift_static
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    Boost::log
    Boost::log_setup
    jaegertracing
    /usr/local/lib/libhiredis.a
)

install(TARGETS HomeTimelineService DESTINATION ./)