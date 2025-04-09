cmake_minimum_required(VERSION 3.10)
project(LSM_TREE)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 添加必要的 include 路径
include_directories(${CMAKE_SOURCE_DIR}/third_party/llama.cpp/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party/llama.cpp)
include_directories(${CMAKE_SOURCE_DIR}/third_party/llama.cpp/ggml/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party/llama.cpp/common)

# 定义 correctness 可执行文件
add_executable(correctness
        correctness.cc
        kvstore_api.h kvstore.h
        kvstore.cc
        skiplist.cpp skiplist.h
        sstable.cpp sstable.h
        bloom.cpp bloom.h
        MurmurHash3.h utils.h test.h
        sstablehead.cpp sstablehead.h
)
target_link_libraries(correctness embedding)

# 定义 persistence 可执行文件
add_executable(persistence
        persistence.cc
        kvstore_api.h kvstore.h
        kvstore.cc
        skiplist.cpp skiplist.h
        sstable.cpp sstable.h
        bloom.cpp bloom.h
        MurmurHash3.h utils.h test.h
        sstablehead.cpp sstablehead.h
)
target_link_libraries(persistence embedding)

# 定义 E2E_test 可执行文件
add_executable(E2E_test
        test/E2E_test.cpp
        kvstore_api.h kvstore.h
        kvstore.cc
        skiplist.cpp skiplist.h
        sstable.cpp sstable.h
        bloom.cpp bloom.h
        MurmurHash3.h utils.h test.h
        sstablehead.cpp sstablehead.h
)
target_link_libraries(E2E_test embedding)

## 定义 Timing_Test 可执行文件
#add_executable(
#        Timing_Test
#        test/Timing_Test.cpp
#        kvstore_api.h kvstore.h
#        kvstore.cc
#        skiplist.cpp skiplist.h
#        sstable.cpp sstable.h
#        bloom.cpp bloom.h
#        MurmurHash3.h utils.h test.h
#        sstablehead.cpp sstablehead.h
#)
#target_link_libraries(Timing_Test embedding)

# 添加子目录
add_subdirectory(third_party/llama.cpp)
add_subdirectory(embedding)
add_subdirectory(test)
