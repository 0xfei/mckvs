cmake_minimum_required(VERSION 3.12)
project(tbkvs)

set(CMAKE_CXX_STANDARD 11)

include_directories(src)

add_executable(tbkvs main.cpp
        src/mem/base_block.h
        src/mem/sklist_block.cpp
        src/mem/sklist_block.h
        src/common/rwlock.h src/server.h src/common/rwlock_test.cpp src/mem/mem_table.h src/common/convert.h)
