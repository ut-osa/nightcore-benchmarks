cmake_minimum_required(VERSION 3.5)
project(social_network_microservices)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "-O3")
set(CMAKE_INSTALL_PREFIX /usr/local/bin)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(BUILD_COMPILER OFF CACHE BOOL "")
set(BUILD_TESTING OFF CACHE BOOL "")
set(WITH_C_GLIB OFF CACHE BOOL "")
set(WITH_JAVA OFF CACHE BOOL "")
set(WITH_PYTHON OFF CACHE BOOL "")
set(WITH_HASKELL OFF CACHE BOOL "")
set(WITH_SHARED_LIB OFF CACHE BOOL "")
add_subdirectory(thrift)

set(THRIFT_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/thrift/lib/cpp/src ${CMAKE_BINARY_DIR}/thrift)

add_subdirectory(libmc)
add_subdirectory(src)
#add_subdirectory(test)
#enable_testing()


