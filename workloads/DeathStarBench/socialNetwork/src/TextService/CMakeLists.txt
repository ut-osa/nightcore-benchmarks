add_library(
    TextService SHARED
    TextService.cpp
    ${THRIFT_GEN_CPP_DIR}/TextService.cpp
    ${THRIFT_GEN_CPP_DIR}/ComposePostService.cpp
    ${THRIFT_GEN_CPP_DIR}/UserMentionService.cpp
    ${THRIFT_GEN_CPP_DIR}/UrlShortenService.cpp
    ${THRIFT_GEN_CPP_DIR}/social_network_types.cpp
)

add_definitions( -DTHRIFT_USE_NON_BLOCKING_SERVER )

target_include_directories(
    TextService PRIVATE
    ${THRIFT_INCLUDE_DIRS}
    /usr/local/include/jaegertracing
)

target_link_libraries(
    TextService
    nlohmann_json::nlohmann_json
    thrift_static
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    Boost::log
    Boost::log_setup
    jaegertracing
)

install(TARGETS TextService DESTINATION ./)