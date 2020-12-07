add_library(
    ComposeReviewService SHARED
    ComposeReviewService.cpp
    ${THRIFT_GEN_CPP_DIR}/ComposeReviewService.cpp
    ${THRIFT_GEN_CPP_DIR}/media_service_types.cpp
    ${THRIFT_GEN_CPP_DIR}/ReviewStorageService.cpp
    ${THRIFT_GEN_CPP_DIR}/UserReviewService.cpp
    ${THRIFT_GEN_CPP_DIR}/MovieReviewService.cpp
)

target_include_directories(
    ComposeReviewService PRIVATE
    ${LIBMEMCACHED_INCLUDE_DIR}
    ${THRIFT_INCLUDE_DIRS}
    /usr/local/include/jaegertracing
)

target_link_libraries(
    ComposeReviewService
    mc
    nlohmann_json::nlohmann_json
    thrift_static
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    Boost::log
    Boost::log_setup
    jaegertracing
)

install(TARGETS ComposeReviewService DESTINATION ./)