/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  IndexIterator(int index, B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, BufferPoolManager *buffer_pool_manager);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

private:
  void UnlockAndUnPin() {
    buffer_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false); // fetch two times
  }
  int index_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_;
  BufferPoolManager *buffer_pool_manager_;
};

} // namespace cmudb
