cmake_minimum_required(VERSION 3.10)
project(tbkvs)

set(CMAKE_CXX_STANDARD 11)

include_directories(src)

link_libraries(libprotobuf.a)

add_executable(tbkvs main.cpp
        src/common/rwlock.h
        src/common/convert.h
        src/memory/base_block.h
        src/memory/sklist_block.h
        src/memory/mem_table.h
        src/binlog/binlog.cpp
        src/binlog/binlog.h
        src/proto/request.pb.cc
        src/proto/request.pb.h
        src/server.h src/common/list.h src/binlog/memlog.cpp src/binlog/memlog.h src/handler/monitor.cpp src/handler/monitor.h src/handler/worker.cpp src/handler/worker.h src/handler/server.cpp src/handler/server.h)
