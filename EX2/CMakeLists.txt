#cmake_minimum_required(VERSION 3.10)

## set the project name
#project(EX2)
#
## add the executable
#add_executable(test EX2/uthreads.h EX2/uthreads.cpp EX2/jona_tests/jona1.cpp)


cmake_minimum_required(VERSION 3.1)
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
project(threads VERSION 1.0 LANGUAGES C CXX)

add_library(uthreads uthreads.h uthreads.cpp)

set_property(TARGET uthreads PROPERTY CXX_STANDARD 11)
target_compile_options(uthreads PUBLIC -Wall -Wextra)

add_subdirectory(tests)