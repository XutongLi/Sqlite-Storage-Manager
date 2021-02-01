#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size)
    : maxBucketSize(size), globalDepth(0), numBuckets(1) {
        table.push_back(std::make_shared<Bucket>(0, size, 0));
    }

/*
 * helper function to calculate the hashing address of input key
 * Different hash function results in different offset.
 * There is localDepth test in test code, so I have to use std::hash.
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
//    std::unique_ptr<char[]> buf(new char[sizeof(key)]);
//    memcpy(buf.get(), &key, sizeof(key));
//    return CityHash32(buf.get(), sizeof(key)) & ((1 << globalDepth) - 1);
    return std::hash<K>{}(key) & ((1 << globalDepth) - 1);
}

/*
 * helper function to return global depth of hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
    return globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
    if (bucket_id >= static_cast<int>(table.size()))
        return -1;
    return table[bucket_id]->localDepth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
    return numBuckets;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
    std::unique_lock<std::mutex> tableLock(mutex);
    size_t index = HashKey(key);
    for (auto entry : table[index]->entries) {
        if (entry.flag == HASVAL && entry.key == key) {
            value = entry.value;
            return true;
        }
    }
    return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
    std::unique_lock<std::mutex> tableLock(mutex);
    size_t index = HashKey(key);
    for (auto &entry : table[index]->entries) {
        if (entry.flag == HASVAL && entry.key == key) {
            entry.flag = NONE;
            -- table[index]->size;
            return true;
        }
    }
    return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
    std::unique_lock<std::mutex> tableLock(mutex);
    size_t index = HashKey(key);
    std::shared_ptr<Bucket> insertBucket = table[index];
    for (auto &entry : insertBucket->entries) {
        if (entry.flag == HASVAL && entry.key == key) { // update
            entry.value = value;
            return;
        }
    }

    while (insertBucket->size == insertBucket->maxBucketSize) {
        if (insertBucket->localDepth == globalDepth) {  // increase size of table
            int length = table.size();
            for (int i = 0; i < length; ++ i) {
                table.push_back(table[i]);  // offsets with the same suffix point to the same bucket
            }
            ++ globalDepth;
        }
        size_t indexSt0 = index;    // index started with 0
        size_t indexSt1 = index ^ (1 << insertBucket->localDepth);  // index started with 1
        table[indexSt1] = std::make_shared<Bucket>(0, maxBucketSize, 0);
        int localDepth = ++ table[indexSt0]->localDepth;
        table[indexSt1]->localDepth = localDepth;
        for (auto &entry : table[indexSt0]->entries) {  // redistribute the old bucket
            if (entry.flag == NONE) continue;
            size_t curIndex = HashKey(entry.key);
            if ((curIndex & (1 << (localDepth - 1))) == (indexSt0 & (1 << (localDepth - 1))))   continue;
            table[indexSt1]->entries[table[indexSt1]->size] = BucketEntry(entry.key, entry.value);
            ++ table[indexSt1]->size;
            -- table[indexSt0]->size;
            entry.flag = NONE;
        }
        ++ numBuckets;
        index = HashKey(key);
        insertBucket = table[index];
    }
    for (auto &entry : insertBucket->entries) { // insert
        if (entry.flag == NONE) {
            entry.key = key;
            entry.value = value;
            entry.flag = HASVAL;
            ++ insertBucket->size;
            return;
        }
    }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
