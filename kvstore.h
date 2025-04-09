#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "sstable.h"
#include "sstablehead.h"
#include "embedding.h"
#include <map>
#include <set>
#include <vector>

class KVStore : public KVStoreAPI {
private:
    skiplist *s = new skiplist(0.5);
    std::vector<sstablehead> sstableIndex[15];
    std::map<uint64_t, std::vector<float>> embeddings;

    bool out_of_limit(int curLevel);
    int totalLevel = -1;

    std::vector<float> generate_embedding(const std::string &value);

public:
    KVStore(const std::string &dir);
    ~KVStore();

    void put(uint64_t key, const std::string &s) override;
    std::string get(uint64_t key) override;
    bool del(uint64_t key) override;
    void reset() override;
    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;
    void compaction();
    void delsstable(std::string filename);
    void addsstable(sstable ss, int level);
    std::string fetchString(std::string file, int startOffset, uint32_t len);

    std::vector<std::pair<std::uint64_t, std::string>> search_knn(std::string query, int k);
};