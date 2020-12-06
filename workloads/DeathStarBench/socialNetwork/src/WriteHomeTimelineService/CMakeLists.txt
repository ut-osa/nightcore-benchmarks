add_executable(
    WriteHomeTimelineService
    WriteHomeTimelineService.cpp
    ${THRIFT_GEN_CPP_DIR}/SocialGraphService.cpp
    ${THRIFT_GEN_CPP_DIR}/social_network_types.cpp
)

target_include_directories(
    WriteHomeTimelineService PRIVATE
    ${THRIFT_INCLUDE_DIRS}
    /usr/local/include/jaegertracing
    ${LIBEVENT_INCLUDE_DIRS}
)


target_link_libraries(
    WriteHomeTimelineService
    thrift_static
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    nlohmann_json::nlohmann_json
    Boost::log
    Boost::log_setup
    OpenSSL::SSL
    /usr/local/lib/libjaegertracing.so
    /usr/local/lib/libamqpcpp.so
    ${LIBEVENT_LIBRARIES}
    /usr/local/lib/libhiredis.a
)

install(TARGETS WriteHomeTimelineService DESTINATION ./)