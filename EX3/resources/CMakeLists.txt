cmake_minimum_required(VERSION 3.13)
project(EX3)

#set (CMAKE_CXX_FLAGS_DEBUG  "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
#set (CMAKE_LINKER_FLAGS_DEBUG  "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")

set(CMAKE_CXX_STANDARD 11)
add_executable(ex3 SampleClient/SampleClient.cpp MapReduceFramework.h MapReduceFramework.cpp MapReduceClient.h Barrier.cpp Barrier.h)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(ex3 Threads::Threads)
