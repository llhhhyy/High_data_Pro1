#include "kvstore.h"
#include <chrono>
#include <iostream>
#include <random>
#include <vector>
#include <iomanip>

using namespace std;

// 测试参数
const int NUM_OPERATIONS = 10000;  // 每个操作的测试次数
const int VALUE_SIZE = 100;        // 值的大小（字节）
const string DATA_DIR = "./data/"; // 数据存储目录

// 生成随机字符串
string generateRandomString(size_t length) {
    static const string chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static random_device rd;
    static mt19937 generator(rd());
    uniform_int_distribution<> distribution(0, chars.size() - 1);
    
    string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        result += chars[distribution(generator)];
    }
    return result;
}

// 测试函数结构
struct TestResult {
    double throughput;  // 每秒操作次数
    double latency;     // 平均时延（微秒）
};

// 测试 put 操作
TestResult testPut(KVStore& store) {
    vector<pair<uint64_t, string>> testData;
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<uint64_t> dis(1, UINT64_MAX);
    
    // 生成测试数据
    for (int i = 0; i < NUM_OPERATIONS; ++i) {
        testData.emplace_back(dis(gen), generateRandomString(VALUE_SIZE));
    }
    
    auto start = chrono::high_resolution_clock::now();
    
    // 执行 put 操作
    for (const auto& item : testData) {
        store.put(item.first, item.second);
    }
    
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start).count();
    
    TestResult result;
    result.throughput = static_cast<double>(NUM_OPERATIONS) / (duration / 1000000.0);
    result.latency = static_cast<double>(duration) / NUM_OPERATIONS;
    
    return result;
}

// 测试 get 操作
TestResult testGet(KVStore& store, const vector<pair<uint64_t, string>>& testData) {
    auto start = chrono::high_resolution_clock::now();
    
    // 执行 get 操作
    for (const auto& item : testData) {
        string value = store.get(item.first);
    }
    
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start).count();
    
    TestResult result;
    result.throughput = static_cast<double>(NUM_OPERATIONS) / (duration / 1000000.0);
    result.latency = static_cast<double>(duration) / NUM_OPERATIONS;
    
    return result;
}

// 测试 del 操作
TestResult testDel(KVStore& store, const vector<pair<uint64_t, string>>& testData) {
    auto start = chrono::high_resolution_clock::now();
    
    // 执行 del 操作
    for (const auto& item : testData) {
        store.del(item.first);
    }
    
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start).count();
    
    TestResult result;
    result.throughput = static_cast<double>(NUM_OPERATIONS) / (duration / 1000000.0);
    result.latency = static_cast<double>(duration) / NUM_OPERATIONS;
    
    return result;
}

int main() {
    // 初始化 KVStore
    KVStore store(DATA_DIR);
    
    cout << fixed << setprecision(2);
    cout << "Testing KVStore Performance (" << NUM_OPERATIONS << " operations each)\n";
    cout << "Value size: " << VALUE_SIZE << " bytes\n";
    cout << "----------------------------------------\n";
    
    // 测试 put
    cout << "Testing put operations...\n";
    TestResult putResult = testPut(store);
    cout << "Put Throughput: " << putResult.throughput << " ops/sec\n";
    cout << "Put Avg Latency: " << putResult.latency << " us/op\n\n";
    
    // 获取测试数据用于后续测试
    vector<pair<uint64_t, string>> testData;
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<uint64_t> dis(1, UINT64_MAX);
    for (int i = 0; i < NUM_OPERATIONS; ++i) {
        testData.emplace_back(dis(gen), generateRandomString(VALUE_SIZE));
        store.put(testData[i].first, testData[i].second);
    }
    
    // 测试 get
    cout << "Testing get operations...\n";
    TestResult getResult = testGet(store, testData);
    cout << "Get Throughput: " << getResult.throughput << " ops/sec\n";
    cout << "Get Avg Latency: " << getResult.latency << " us/op\n\n";
    
    // 测试 del
    cout << "Testing del operations...\n";
    TestResult delResult = testDel(store, testData);
    cout << "Del Throughput: " << delResult.throughput << " ops/sec\n";
    cout << "Del Avg Latency: " << delResult.latency << " us/op\n";
    
    // 重置存储以清理测试数据
    store.reset();
    
    return 0;
}