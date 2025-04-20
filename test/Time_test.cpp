#include "../kvstore.h"
#include <chrono>
#include <iostream>
#include <random>
#include <vector>
#include <string>
#include <list>

const int NUM_OPERATIONS = 50; // 每个操作执行次数
const int KNN_K = 5; // search_knn 的 k 值

// 生成随机字符串
std::string generate_random_string(size_t length) {
    const std::string characters = "abcdefghijklmnopqrstuvwxyz";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, characters.size() - 1);
    std::string result;
    for (size_t i = 0; i < length; ++i) {
        result += characters[dis(gen)];
    }
    return result;
}

// 测试函数
void run_timing_tests() {
    KVStore store("./data"); // 创建 KVStore 实例，数据存储在 ./data 目录
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> key_dist(1, 1000000); // 随机键范围
    std::vector<std::pair<uint64_t, std::string>> test_data;

    // 准备测试数据
    std::cout << "Preparing test data..." << std::endl;
    for (int i = 0; i < NUM_OPERATIONS; ++i) {
        uint64_t key = key_dist(gen);
        std::string value = generate_random_string(10); // 随机生成 10 字符的 value
        test_data.emplace_back(key, value);
        store.put(key, value); // 预先插入数据
    }

    // 1. 测试 put
    double put_total_time = 0;
    for (int i = 0; i < NUM_OPERATIONS; ++i) {
        uint64_t key = key_dist(gen);
        std::string value = generate_random_string(10);
        auto start = std::chrono::high_resolution_clock::now();
        store.put(key, value);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        put_total_time += duration.count();
    }
    double put_avg_time = put_total_time / NUM_OPERATIONS;

    // 2. 测试 get
    double get_total_time = 0;
    for (int i = 0; i < NUM_OPERATIONS; ++i) {
        uint64_t key = test_data[i % test_data.size()].first; // 从已有数据中取键
        auto start = std::chrono::high_resolution_clock::now();
        store.get(key);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        get_total_time += duration.count();
    }
    double get_avg_time = get_total_time / NUM_OPERATIONS;

    // 3. 测试 del
    double del_total_time = 0;
    for (int i = 0; i < NUM_OPERATIONS; ++i) {
        uint64_t key = test_data[i % test_data.size()].first;
        auto start = std::chrono::high_resolution_clock::now();
        store.del(key);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        del_total_time += duration.count();
    }
    double del_avg_time = del_total_time / NUM_OPERATIONS;

    // 4. 测试 scan
    double scan_total_time = 0;
    for (int i = 0; i < NUM_OPERATIONS; ++i) {
        uint64_t key1 = test_data[i % test_data.size()].first;
        uint64_t key2 = key1 + 100; // 扫描一个范围
        std::list<std::pair<uint64_t, std::string>> result;
        auto start = std::chrono::high_resolution_clock::now();
        store.scan(key1, key2, result);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        scan_total_time += duration.count();
    }
    double scan_avg_time = scan_total_time / NUM_OPERATIONS;

    // 5. 测试 search_knn
    double knn_total_time = 0;
    for (int i = 0; i < NUM_OPERATIONS; ++i) {
        std::string query = generate_random_string(10);
        auto start = std::chrono::high_resolution_clock::now();
        store.search_knn(query, KNN_K);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        knn_total_time += duration.count();
    }
    double knn_avg_time = knn_total_time / NUM_OPERATIONS;

    // 6. 测试 compaction（需要足够数据触发）
    double compaction_total_time = 0;
    int compaction_runs = 10; // 减少运行次数，因为 compaction 耗时较长
    for (int i = 0; i < compaction_runs; ++i) {
        // 插入更多数据以触发 compaction
        for (int j = 0; j < 100; ++j) {
            store.put(key_dist(gen), generate_random_string(100)); // 插入较大 value
        }
        auto start = std::chrono::high_resolution_clock::now();
        store.compaction();
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        compaction_total_time += duration.count();
    }
    double compaction_avg_time = compaction_total_time / compaction_runs;

    // 输出结果
    std::cout << "\n=== Timing Test Results ===" << std::endl;
    std::cout << "Operation    | Avg Time (microseconds)" << std::endl;
    std::cout << "-------------|-----------------------" << std::endl;
    std::cout << "put          | " << put_avg_time << std::endl;
    std::cout << "get          | " << get_avg_time << std::endl;
    std::cout << "del          | " << del_avg_time << std::endl;
    std::cout << "scan         | " << scan_avg_time << std::endl;
    std::cout << "search_knn   | " << knn_avg_time << std::endl;
    std::cout << "compaction   | " << compaction_avg_time << std::endl;
}

int main() {
    std::cout << "Starting KVStore timing tests..." << std::endl;
    run_timing_tests();
    std::cout << "Tests completed." << std::endl;
    return 0;
}