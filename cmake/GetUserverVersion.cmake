string(TIMESTAMP USERVER_BUILD_TIME %Y%m%d%H%M%S UTC)

set(USERVER_HASH "unknown")

find_package(Git)

if(Git_FOUND)
  execute_process(
    COMMAND ${Git_EXECUTABLE} rev-parse --short HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE USERVER_HASH
    ERROR_QUIET
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
else()
  message(STATUS "Git not found")
endif()

set(USERVER_VERSION "1.0.0")
