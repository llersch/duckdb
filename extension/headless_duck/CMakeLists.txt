cmake_minimum_required(VERSION 3.5...3.29)

project(HeadlessDuckExtension)

include_directories(include)

set(HEADLESS_DUCK_EXTENSION_FILES
    headless_duck_extension.cpp
    headless_duck_metadata.cpp
    headless_duck_writer.cpp
    headless_duck_reader.cpp
    headless_duck_block_manager.cpp)

build_static_extension(headless_duck ${HEADLESS_DUCK_EXTENSION_FILES})
build_loadable_extension(headless_duck "" ${HEADLESS_DUCK_EXTENSION_FILES})

install(
  TARGETS headless_duck_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
