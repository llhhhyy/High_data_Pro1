#include "skiplist.h"

#include <iostream>
#include <random>

double skiplist::my_rand() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_real_distribution<> dis(0.0, 1.0);
    return dis(gen);
}

int skiplist::randLevel() {
    int level = 1;
    while (my_rand() < p && level < MAX_LEVEL) {
        level++;
    }
    return level;
}

void skiplist::insert(uint64_t key, const std::string &str) {
    std::vector<slnode *> update(MAX_LEVEL, nullptr);
    slnode *current = head;

    for (int i = curMaxL - 1; i >= 0; i--) {
        while (current->nxt[i] != tail && current->nxt[i]->key < key) {
            current = current->nxt[i];
        }
        update[i] = current; // 前驱节点
    }

    current = current->nxt[0];
    if (current != tail && current->key == key) {
        bytes -= current->val.length();
        current->val = str;
        bytes += str.length();
        return;
    }

    int newlevel    = randLevel();
    slnode *newNode = new slnode(key, str, NORMAL);
    bytes += sizeof(slnode) + str.length();

    if (newlevel > curMaxL) {
        for (int i = curMaxL; i < newlevel; i++) {
            update[i] = head;
        }
        curMaxL = newlevel;
    }

    for (int i = 0; i < newlevel; i++) {
        newNode->nxt[i]   = update[i]->nxt[i];
        update[i]->nxt[i] = newNode;
    }
}

std::string skiplist::search(uint64_t key) {
    slnode *current = head;

    for (int i = curMaxL - 1; i >= 0; i--) {
        while (current->nxt[i] != tail && current->nxt[i]->key < key) {
            current = current->nxt[i];
        }
    }

    current = current->nxt[0];
    if (current != tail && current->key == key) {
        return current->val;
    }
    return "";
}

bool skiplist::del(uint64_t key, uint32_t len) {
    std::vector<slnode *> update(MAX_LEVEL, nullptr);
    slnode *current = head;

    for (int i = curMaxL - 1; i >= 0; i--) {
        while (current->nxt[i] != tail && current->nxt[i]->key < key) {
            current = current->nxt[i];
        }
        update[i] = current;
    }
    current = current->nxt[0];
    if (current == tail || current->key != key) {
        return false;
    }
    bytes -= (sizeof(slnode) + current->val.length());

    for (int i = 0; i < curMaxL; i++) {
        if (update[i]->nxt[i] != current) {
            break;
        }
        update[i]->nxt[i] = current->nxt[i];
    }

    while (curMaxL > 1 && head->nxt[curMaxL - 1] == tail) {
        curMaxL--;
    }
    delete current;
    return true;
}

void skiplist::scan(uint64_t key1, uint64_t key2, std::vector<std::pair<uint64_t, std::string>> &list) {
    slnode *current = lowerBound(key1);

    while (current != tail && current->key <= key2) {
        list.push_back({current->key, current->val});
        current = current->nxt[0];
    }
}

slnode *skiplist::lowerBound(uint64_t key) {
    slnode *current = head;
    for (int i = curMaxL - 1; i >= 0; i--) {
        while (current->nxt[i] != tail && current->nxt[i]->key < key) {
            current = current->nxt[i];
        }
    }
    return current->nxt[0];
}

void skiplist::reset() {
    slnode *current = head->nxt[0];
    while (current != tail) {
        slnode *next = current->nxt[0];
        delete current;
        current = next;
    }

    for (int i = 0; i < MAX_LEVEL; i++) {
        head->nxt[i] = tail;
    }
    bytes   = 0;
    curMaxL = 1;
    s       = 1;
}

uint32_t skiplist::getBytes() {
    return bytes;
}
