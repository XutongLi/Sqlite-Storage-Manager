/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <queue>
#include <vector>

#include "concurrency/transaction.h"
#include "index/index_iterator.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"

// #define DBG

namespace cmudb {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>
enum class LockType { EXCLUSIVE = 0, SHARED };

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
public:
  explicit BPlusTree(const std::string &name,
                           BufferPoolManager *buffer_pool_manager,
                           const KeyComparator &comparator,
                           page_id_t root_page_id = INVALID_PAGE_ID);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value,
              Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> &result,
                Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE Begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);

  // Print this B+ tree to stdout using a simple command-line
  std::string ToString(bool verbose = false);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);
  // expose for test purpose
  B_PLUS_TREE_LEAF_PAGE_TYPE *FindLeafPage(const KeyType &key, bool leftMost = false, 
                                            OpType op = OpType::READ, Transaction *transaction = nullptr);

  bool Check(bool force = false);
  bool openCheck = true;

private:
  void StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value,
                      Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
                        BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  template <typename N> N *Split(N *node, Transaction *transaction = nullptr);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  template <typename N>
  bool FindSibling(N *node, N * &sibling, Transaction *transaction = nullptr);

  template <typename N>
  bool Coalesce(
      N *&neighbor_node, N *&node,
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
      int index, Transaction *transaction = nullptr);

  template <typename N> void Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = false);

  void RemovePagesInTransaction(LockType lock_type, Transaction *transaction, page_id_t cur_id = INVALID_PAGE_ID);

  BPlusTreePage *ConcurrentFetchPage(page_id_t page_id, OpType op, page_id_t previous_id, Transaction *transaction);

  inline void LockPage(LockType lock_type, Page *page) {
    if (lock_type == LockType::EXCLUSIVE)
      page->WLatch();
    else if (lock_type == LockType::SHARED)
      page->RLatch();
  }

  inline void UnlockPage(LockType lock_type, Page *page) {
    if (lock_type == LockType::EXCLUSIVE)
      page->WUnlatch();
    else if (lock_type == LockType::SHARED)
      page->RUnlatch();
  }

  inline void LockRootPage(LockType lock_type) {
    if (lock_type == LockType::EXCLUSIVE)
      rw_mutex_.WLock();
    else if (lock_type == LockType::SHARED)
      rw_mutex_.RLock();
    ++ root_locked_cnt;
  }

  inline void UnlockRootPage(LockType lock_type) {
    if (root_locked_cnt == 0)
      return;
    if (lock_type == LockType::EXCLUSIVE)
      rw_mutex_.WUnlock();
    else if (lock_type == LockType::SHARED)
      rw_mutex_.RUnlock();
    -- root_locked_cnt;
  }

  inline void UnlockPage(LockType lock_type, page_id_t page_id) {
    auto page = buffer_pool_manager_->FetchPage(page_id);
    UnlockPage(lock_type, page);
    buffer_pool_manager_->UnpinPage(page_id, lock_type == LockType::EXCLUSIVE);
  }

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  RWMutex rw_mutex_;  // protect root_page_id_
  static thread_local int root_locked_cnt;
};

} // namespace cmudb
