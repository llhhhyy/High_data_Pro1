#include "kvstore.h"

#include "skiplist.h"
#include "sstable.h"
#include "utils.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <queue>
#include <set>
#include <string>
#include <utility>

static const std::string DEL = "~DELETED~";
const uint32_t MAXSIZE       = 2 * 1024 * 1024;

struct poi {
    int sstableId; // vector中第几个sstable
    int pos;       // 该sstable的第几个key-offset
    uint64_t time;
    Index index;
};

struct cmpPoi {
    bool operator()(const poi &a, const poi &b) {
        if (a.index.key == b.index.key)
            return a.time < b.time;
        return a.index.key > b.index.key;
    }
};

KVStore::KVStore(const std::string &dir) :
    KVStoreAPI(dir) // read from sstables
{
    for (totalLevel = 0;; ++totalLevel) {
        std::string path = dir + "/level-" + std::to_string(totalLevel) + "/";
        std::vector<std::string> files;
        if (!utils::dirExists(path)) {
            totalLevel--;
            break; // stop read
        }
        int nums = utils::scanDir(path, files);
        sstablehead cur;
        for (int i = 0; i < nums; ++i) {       // 读每一个文件头
            std::string url = path + files[i]; // url, 每一个文件名
            cur.loadFileHead(url.data());
            sstableIndex[totalLevel].push_back(cur);
            TIME = std::max(TIME, cur.getTime()); // 更新时间戳
        }
    }
}

KVStore::~KVStore() {
    sstable ss(s);
    if (!ss.getCnt())
        return; // empty sstable
    std::string path = std::string("./data/level-0/");
    if (!utils::dirExists(path)) {
        utils::_mkdir(path.data());
        totalLevel = 0;
    }
    ss.putFile(ss.getFilename().data());
    compaction(); // 从0层开始尝试合并
}

bool KVStore::out_of_limit(int curLevel) {
    int limit = 1 << (curLevel + 1);
    if (sstableIndex[curLevel].size() > limit)
        return true;
    return false;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &val) {
    auto start = std::chrono::high_resolution_clock::now(); // 计时开始
    std::vector<float> embedding_vector = embedding_single(val);

    // 将向量存储到内存中
    embeddings[key] = embedding_vector;

    embeddings[key]  = generate_embedding(val);
    uint32_t nxtsize = s->getBytes();
    std::string res  = s->search(key);
    if (!res.length()) { // 新增
        nxtsize += 12 + val.length();
    } else
        nxtsize = nxtsize - res.length() + val.length();
    if (nxtsize + 10240 + 32 <= MAXSIZE) {
        s->insert(key, val);
    } else {
        sstable ss(s);
        s->reset();
        std::string url  = ss.getFilename();
        std::string path = "./data/level-0";
        if (!utils::dirExists(path)) {
            utils::mkdir(path.data());
            totalLevel = 0;
        }
        addsstable(ss, 0);      // 加入缓存
        ss.putFile(url.data()); // 加入磁盘
        compaction();
        s->insert(key, val);
    }

    auto end = std::chrono::high_resolution_clock::now(); // 计时结束
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "put operation took " << duration.count() << " microseconds" << std::endl;
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
    auto start = std::chrono::high_resolution_clock::now(); // 计时开始

    uint64_t time = 0;
    int goalOffset;
    uint32_t goalLen;
    std::string goalUrl;
    std::string res = s->search(key);
    if (res.length()) {
        if (res == DEL)
            return "";
        return res;
    }
    for (int level = 0; level <= totalLevel; ++level) {
        for (sstablehead it : sstableIndex[level]) {
            if (key < it.getMinV() || key > it.getMaxV())
                continue;
            uint32_t len;
            int offset = it.searchOffset(key, len);
            if (offset == -1) {
                if (!level)
                    continue;
                else
                    break;
            }
            if (it.getTime() > time) {
                time       = it.getTime();
                goalUrl    = it.getFilename();
                goalOffset = offset + 32 + 10240 + 12 * it.getCnt();
                goalLen    = len;
            }
        }
        if (time)
            break;
    }
    if (!goalUrl.length())
        return "";
    res = fetchString(goalUrl, goalOffset, goalLen);
    if (res == DEL)
        return "";

    auto end = std::chrono::high_resolution_clock::now(); // 计时结束
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "get operation took " << duration.count() << " microseconds" << std::endl;
    return res;
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    auto start = std::chrono::high_resolution_clock::now(); // 计时开始

    std::string res = get(key);
    if (!res.length())
        return false;
    embeddings.erase(key);
    put(key, DEL);

    auto end = std::chrono::high_resolution_clock::now(); // 计时结束
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "del operation took " << duration.count() << " microseconds" << std::endl;
    return true;
}
/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    s->reset();         // 清空memtable
    embeddings.clear(); // 清空嵌入向量
    std::vector<std::string> files;
    for (int level = 0; level <= totalLevel; ++level) {
        std::string path = std::string("./data/level-") + std::to_string(level);
        int size         = utils::scanDir(path, files);
        for (int i = 0; i < size; ++i) {
            std::string file = path + "/" + files[i];
            utils::rmfile(file.data());
        }
        utils::rmdir(path.data());
        sstableIndex[level].clear();
    }
    totalLevel = -1;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */

struct myPair {
    uint64_t key, time;
    int id, index;
    std::string filename;

    myPair(uint64_t key, uint64_t time, int index, int id,
           std::string file) { // construct function
        this->time     = time;
        this->key      = key;
        this->id       = id;
        this->index    = index;
        this->filename = file;
    }
};

struct cmp {
    bool operator()(myPair &a, myPair &b) {
        if (a.key == b.key)
            return a.time < b.time;
        return a.key > b.key;
    }
};

void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) {
    auto start = std::chrono::high_resolution_clock::now(); // 计时开始

    std::vector<std::pair<uint64_t, std::string>> mem;
    std::priority_queue<myPair, std::vector<myPair>, cmp> heap;
    std::vector<sstablehead> sshs;
    s->scan(key1, key2, mem);
    std::vector<int> head, end;
    int cnt = 0;
    if (mem.size())
        heap.push(myPair(mem[0].first, INF, 0, -1, "qwq"));
    for (int level = 0; level <= totalLevel; ++level) {
        for (sstablehead it : sstableIndex[level]) {
            if (key1 > it.getMaxV() || key2 < it.getMinV())
                continue;
            int hIndex = it.lowerBound(key1);
            int tIndex = it.lowerBound(key2);
            if (hIndex < it.getCnt()) {
                std::string url = it.getFilename();
                heap.push(myPair(it.getKey(hIndex), it.getTime(), hIndex, cnt++, url));
                head.push_back(hIndex);
                if (it.search(key2) == tIndex)
                    tIndex++;
                end.push_back(tIndex);
                sshs.push_back(it);
            }
        }
    }
    uint64_t lastKey = INF;
    while (!heap.empty()) {
        myPair cur = heap.top();
        heap.pop();
        if (cur.id >= 0) {
            if (cur.key != lastKey) {
                lastKey         = cur.key;
                uint32_t start  = sshs[cur.id].getOffset(cur.index - 1);
                uint32_t len    = sshs[cur.id].getOffset(cur.index) - start;
                uint32_t scnt   = sshs[cur.id].getCnt();
                std::string res = fetchString(cur.filename, 10240 + 32 + scnt * 12 + start, len);
                if (res.length() && res != DEL)
                    list.emplace_back(cur.key, res);
            }
            if (cur.index + 1 < end[cur.id]) {
                heap.push(myPair(sshs[cur.id].getKey(cur.index + 1), cur.time, cur.index + 1, cur.id, cur.filename));
            }
        } else {
            if (cur.key != lastKey) {
                lastKey         = cur.key;
                std::string res = mem[cur.index].second;
                if (res.length() && res != DEL)
                    list.emplace_back(cur.key, mem[cur.index].second);
            }
            if (cur.index < mem.size() - 1) {
                heap.push(myPair(mem[cur.index + 1].first, cur.time, cur.index + 1, -1, cur.filename));
            }
        }
    }

    auto endtime = std::chrono::high_resolution_clock::now(); // 计时结束
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endtime - start);
    std::cout << "scan operation took " << duration.count() << " microseconds" << std::endl;
}

void KVStore::compaction() {
    auto start = std::chrono::high_resolution_clock::now(); // 计时开始

    int curLevel = 0;
    while (out_of_limit(curLevel)) {
        std::string path = std::string("./data/level-") + std::to_string(curLevel + 1);
        if (!utils::dirExists(path)) {
            utils::mkdir(path.data());
        }
        std::vector<sstablehead> toMerge;
        int size = sstableIndex[curLevel].size();
        if (curLevel == 0) {
            size = 3;
        } else {
            size -= 1 << (curLevel + 1);
        }
        for (int i = 0; i < size; i++) {
            toMerge.push_back(sstableIndex[curLevel][i]);
        }
        uint64_t minKey = INF, maxKey = 0;
        for (auto &head : toMerge) {
            minKey = std::min(minKey, head.getMinV());
            maxKey = std::max(maxKey, head.getMaxV());
        }
        for (auto &it : sstableIndex[curLevel + 1]) {
            if (it.getMinV() <= maxKey && it.getMaxV() >= minKey) {
                toMerge.push_back(it);
            }
        }
        std::sort(toMerge.begin(), toMerge.end());
        uint64_t maxTime = toMerge.back().getTime();
        std::map<uint64_t, std::string> mergeMap;
        for (auto &it : toMerge) {
            sstable ss;
            ss.loadFile(it.getFilename().data());
            int cnt = ss.getCnt();
            for (int i = 0; i < cnt; i++) {
                uint64_t key    = ss.getKey(i);
                std::string val = ss.getData(i);
                mergeMap[key]   = val;
            }
            delsstable(it.getFilename());
        }
        sstable newSST;
        uint32_t maxNameSuf = 0;
        for (auto &it : sstableIndex[curLevel + 1]) {
            maxNameSuf = it.getTime() == maxTime ? std::max(maxNameSuf, it.getNameSuf()) : maxNameSuf;
        }
        newSST.setTime(maxTime);
        newSST.setNamesuffix(maxNameSuf);
        for (auto &it : mergeMap) {
            if (newSST.checkSize(it.second, curLevel + 1, 0)) {
                addsstable(newSST, curLevel + 1);
                newSST.reset();
            }
            if (curLevel + 1 == totalLevel && it.second == DEL) {
                continue;
            }
            newSST.insert(it.first, it.second);
        }
        newSST.checkSize("", curLevel + 1, 1);
        addsstable(newSST, curLevel + 1);
        std::sort(sstableIndex[curLevel + 1].begin(), sstableIndex[curLevel + 1].end());
        curLevel++;
    }
    totalLevel = std::max(totalLevel, curLevel);

    auto end = std::chrono::high_resolution_clock::now(); // 计时结束
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "compaction operation took " << duration.count() << " microseconds" << std::endl;
}
void KVStore::delsstable(std::string filename) {
    for (int level = 0; level <= totalLevel; ++level) {
        int size = sstableIndex[level].size(), flag = 0;
        for (int i = 0; i < size; ++i) {
            if (sstableIndex[level][i].getFilename() == filename) {
                sstableIndex[level].erase(sstableIndex[level].begin() + i);
                flag = 1;
                break;
            }
        }
        if (flag)
            break;
    }
    // 检查文件是否存在
    if (utils::dirExists(filename.substr(0, filename.find_last_of('/'))) && std::ifstream(filename).good()) {
        int flag = utils::rmfile(filename.data());
        if (flag != 0) {
            std::cout << "delete fail! " << filename << std::endl;
            std::cout << strerror(errno) << std::endl;
        }
    } else {
        std::cout << "File " << filename << " does not exist, skipping deletion." << std::endl;
    }
}

void KVStore::addsstable(sstable ss, int level) {
    sstableIndex[level].push_back(ss.getHead());
}

char strBuf[2097152];

/**
 * @brief Fetches a substring from a file starting at a given offset.
 *
 * This function opens a file in binary read mode, seeks to the specified start offset,
 * reads a specified number of bytes into a buffer, and returns the buffer as a string.
 *
 * @param file The path to the file from which to read the substring.
 * @param startOffset The offset in the file from which to start reading.
 * @param len The number of bytes to read from the file.
 * @return A string containing the read bytes.
 */
std::string KVStore::fetchString(std::string file, int startOffset, uint32_t len) {
    std::ifstream inFile(file, std::ios::binary);
    if (!inFile) {
        throw std::runtime_error("Failed to open file: " + file);
    }

    inFile.seekg(startOffset, std::ios::beg);
    if (!inFile) {
        throw std::runtime_error("Failed to seek to offset: " + std::to_string(startOffset));
    }

    char *buffer = new char[len];
    inFile.read(buffer, len);
    if (!inFile) {
        delete[] buffer;
        throw std::runtime_error("Failed to read data from file: " + file);
    }

    std::string result(buffer, len);
    delete[] buffer;

    inFile.close();

    return result;
}

std::vector<float> KVStore::generate_embedding(const std::string &value) {
    return embedding_single(value);
}

std::vector<std::pair<std::uint64_t, std::string>> KVStore::search_knn(std::string query, int k) {
    auto start = std::chrono::high_resolution_clock::now(); // 计时开始

    std::vector<std::pair<uint64_t, float>> scores;
    std::vector<std::pair<uint64_t, std::string>> results;
    std::vector<float> query_vec = embedding_single(query);
    if (query_vec.empty())
        return results;
    for (const auto &[key, vec] : embeddings) {
        float dot = 0, norm_a = 0, norm_b = 0;
        for (size_t i = 0; i < vec.size(); ++i) {
            dot += vec[i] * query_vec[i];
            norm_a += vec[i] * vec[i];
            norm_b += query_vec[i] * query_vec[i];
        }
        float similarity = 0;
        if (norm_a > 0 && norm_b > 0) {
            similarity = dot / (sqrt(norm_a) * sqrt(norm_b));
        }
        scores.emplace_back(key, similarity);
    }
    std::sort(
        scores.begin(),
        scores.end(),
        [](const std::pair<uint64_t, float> &a, const std::pair<uint64_t, float> &b) {
            if (a.second != b.second)
                return a.second > b.second;
            return a.first < b.first;
        }
    );
    for (const auto &[key, score] : scores) {
        if (k <= 0)
            break;
        std::string value = get(key);
        if (!value.empty() && value != DEL) {
            results.emplace_back(key, value);
            --k;
        }
    }

    auto end = std::chrono::high_resolution_clock::now(); // 计时结束
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "search_knn operation took " << duration.count() << " microseconds" << std::endl;
    return results;
}
