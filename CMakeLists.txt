# 设置最低 CMake 版本要求
cmake_minimum_required(VERSION 3.10)

# 定义项目名称
project(LSM_KV)

# 设置编译器路径
set(CMAKE_C_COMPILER "D:/gcc14.2/mingw64/bin/gcc.exe")
set(CMAKE_CXX_COMPILER "D:/gcc14.2/mingw64/bin/g++.exe")

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")

# 包含头文件目录
include_directories(${CMAKE_SOURCE_DIR})

# 添加可执行文件目标：correctness
add_executable(correctness
    correctness.cc
    kvstore.cc
    skiplist.cpp
    sstable.cpp
    sstablehead.cpp
    bloom.cpp
)

# 添加可执行文件目标：persistence
add_executable(persistence
    persistence.cc
    kvstore.cc
    skiplist.cpp
    sstable.cpp
    sstablehead.cpp
    bloom.cpp
)

add_executable(Mytest
    Mytest.cc
    kvstore.cc
    skiplist.cpp
    sstable.cpp
    sstablehead.cpp
    bloom.cpp
)