#ifndef LSM_KV_SKIPLIST_H
#define LSM_KV_SKIPLIST_H

#include <cstdint>
#include <limits>
#include <list>
#include <string>
#include <vector>

/**
 * 节点类型的枚举
 * 用于标识跳表中特殊节点和普通节点
 */
enum TYPE {
    HEAD,    // 头部节点，表示跳表的起始点
    NORMAL,  // 普通节点，存储实际的键值对
    TAIL     // 尾部节点，表示跳表的结束点
};

/**
 * 跳表的最大层数常量
 * 18层足以处理大部分应用场景，同时保持内存效率
 * 每层概率递减，实际使用层数通常远小于此值
 */
const int MAX_LEVEL = 18;

/**
 * 跳表节点类
 * 表示跳表中的单个节点，包含键、值和多层指针
 */
class slnode {
public:
    uint64_t key;             // 节点的键，使用64位无符号整数
    std::string val;          // 节点的值，存储字符串数据
    TYPE type;                // 节点类型（HEAD, NORMAL, TAIL）
    std::vector<slnode *> nxt;// 指向下一节点的指针数组，每层一个指针

    /**
     * 构造函数
     * @param key 节点的键
     * @param val 节点的值
     * @param type 节点类型
     * 初始化节点并分配MAX_LEVEL大小的下一节点指针数组
     */
    slnode(uint64_t key, const std::string &val, TYPE type) {
        this->key  = key;
        this->val  = val;
        this->type = type;
        for (int i = 0; i < MAX_LEVEL; ++i)
            nxt.push_back(nullptr); // 初始化所有层指针为nullptr
    }
};

/**
 * 跳表类
 * 实现一个基于概率的多层链表结构，用于高效键值存储
 * 支持插入、删除、查找和范围扫描操作
 */
class skiplist {
private:
    const uint64_t INF = std::numeric_limits<uint64_t>::max(); // 无穷大值，用于尾节点
    double p;          // 层数增长概率，控制跳表的高度分布
    uint64_t s;        // 可能用于种子或计数器，目前未明确使用
    uint32_t bytes;    // 记录索引和数据区域的总字节数，用于内存跟踪
    int curMaxL;       // 当前跳表的最大层数
    slnode *head;      // 头部节点指针，所有查询从此开始
    slnode *tail;      // 尾部节点指针，表示链表的逻辑结束

public:
    /**
     * 构造函数
     * @param p 层数增长概率（0-1之间）
     * 初始化跳表，创建head和tail节点并连接
     */
    skiplist(double p) {
        s       = 1;
        bytes   = 0x0;
        curMaxL = 1;
        this->p = p;
        head    = new slnode(0, "", HEAD);
        tail    = new slnode(INF, "", TAIL);
        for (int i = 0; i < MAX_LEVEL; ++i)
            head->nxt[i] = tail; // 每层head直接指向tail
    }

    /**
     * 获取跳表第一个普通节点
     * @return 第0层的第一个节点指针
     */
    slnode *getFirst() {
        return head->nxt[0];
    }

    /**
     * 生成随机数
     * @return 0到1之间的随机数，用于确定节点层数
     */
    double my_rand();

    /**
     * 随机生成新节点的层数
     * @return 生成的层数（1到MAX_LEVEL之间）
     */
    int randLevel();

    /**
     * 插入键值对
     * @param key 要插入的键
     * @param str 要插入的值
     * 如果键已存在则更新值，否则插入新节点
     */
    void insert(uint64_t key, const std::string &str);

    /**
     * 查找指定键的值
     * @param key 要查找的键
     * @return 对应的值，如果未找到返回空字符串
     */
    std::string search(uint64_t key);

    /**
     * 删除指定键的节点
     * @param key 要删除的键
     * @param len 未明确使用，可能是值的长度
     * @return 是否成功删除
     */
    bool del(uint64_t key, uint32_t len);

    /**
     * 扫描指定范围内的键值对
     * @param key1 范围起点
     * @param key2 范围终点
     * @param list 存储结果的vector
     * 将[key1, key2]范围内的键值对存入list
     */
    void scan(uint64_t key1, uint64_t key2, std::vector<std::pair<uint64_t, std::string>> &list);

    /**
     * 查找第一个大于或等于指定键的节点
     * @param key 目标键
     * @return 对应的节点指针
     */
    slnode *lowerBound(uint64_t key);

    /**
     * 重置跳表到初始状态
     * 删除所有普通节点，恢复初始配置
     */
    void reset();

    /**
     * 获取跳表占用的总字节数
     * @return 索引和数据区域的总字节数
     */
    uint32_t getBytes();
};

#endif // LSM_KV_SKIPLIST_H