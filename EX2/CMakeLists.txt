cmake_minimum_required(VERSION 3.10)

# set the project name
project(EX2)

# add the executable
add_executable(test uthreads.h uthreads.cpp tests/test7_no_out_different.cpp)