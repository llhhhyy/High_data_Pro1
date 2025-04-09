# 添加必要的 include 路径
include_directories(${CMAKE_SOURCE_DIR}/third_party/llama.cpp/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party/llama.cpp)
include_directories(${CMAKE_SOURCE_DIR}/third_party/llama.cpp/ggml/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party/llama.cpp/common)

# 添加 Embedding_Test 可执行文件
add_executable(Embedding_Test Embedding_Test.cpp)
target_link_libraries(Embedding_Test PUBLIC embedding)


