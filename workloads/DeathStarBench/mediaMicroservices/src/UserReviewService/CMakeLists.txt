add_library(
    UserReviewService SHARED
    UserReviewService.cpp
    ${THRIFT_GEN_CPP_DIR}/UserReviewService.cpp
    ${THRIFT_GEN_CPP_DIR}/ReviewStorageService.cpp
    ${THRIFT_GEN_CPP_DIR}/media_service_types.cpp
)

target_include_directories(
    UserReviewService PRIVATE
    ${MONGOC_INCLUDE_DIRS}
    ${THRIFT_INCLUDE_DIRS}
    /usr/local/include/jaegertracing
)

target_link_libraries(
    UserReviewService
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

install(TARGETS UserReviewService DESTINATION ./)