/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <memory>
#include <mutex>
#include <thread>
#include <iostream>

#include "hash/hash_table.h"
#include "hash/city.h"

#define HASVAL 1
#define NONE 0
#define DEBUG

namespace cmudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
      // constructor
      ExtendibleHash(size_t size);
      // helper function to generate hash addressing
      size_t HashKey(const K &key);
      // helper function to get global & local depth
      int GetGlobalDepth() const;
      int GetLocalDepth(int bucket_id) const;
      int GetNumBuckets() const;
      // lookup and modifier
      bool Find(const K &key, V &value) override;
      bool Remove(const K &key) override;
      void Insert(const K &key, const V &value) override;

private:
    struct BucketEntry {
        BucketEntry() : flag(0) {}
        BucketEntry(const K &_key, const V &_value) : flag(1), key(_key), value(_value) {}
        int flag;   // 0 - empty, 1 - has value
        K key;
        V value;
    };
    struct Bucket {
        Bucket(int _localDepth, size_t _maxBucketSize, size_t _size)
            : localDepth(_localDepth), maxBucketSize(_maxBucketSize), size(_size) {
            entries.assign(maxBucketSize, BucketEntry());
        };
        std::vector<BucketEntry> entries;
        int localDepth;
        size_t maxBucketSize;
        size_t size;
    };
    // private members
    size_t maxBucketSize;
    int globalDepth;
    int numBuckets;
    std::vector<std::shared_ptr<Bucket>> table;
    std::mutex mutex;
};
} // namespace cmudb
