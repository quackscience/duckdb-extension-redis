cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME redis)

# DuckDB's extension distribution supports vcpkg
find_package(Boost REQUIRED COMPONENTS system)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/redis_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link against Boost
target_link_libraries(${EXTENSION_NAME} Boost::system)
target_link_libraries(${LOADABLE_EXTENSION_NAME} Boost::system)

# Define REDISCPP_HEADER_ONLY to use redis-cpp as header-only
target_compile_definitions(${EXTENSION_NAME} PRIVATE REDISCPP_HEADER_ONLY)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE REDISCPP_HEADER_ONLY)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
