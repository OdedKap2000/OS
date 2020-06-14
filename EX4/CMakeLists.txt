cmake_minimum_required(VERSION 3.13)
project(EX4)

# set (CMAKE_CXX_FLAGS_DEBUG  "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
# set (CMAKE_LINKER_FLAGS_DEBUG  "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")

set(CMAKE_CXX_STANDARD 11)
add_executable(ex4 MemoryConstants.h PhysicalMemory.cpp PhysicalMemory.h SimpleTest.cpp VirtualMemory.cpp VirtualMemory.h)

