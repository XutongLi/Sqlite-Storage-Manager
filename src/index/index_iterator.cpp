/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(int index, B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, BufferPoolManager *buffer_pool_manager) 
                        : index_(index), leaf_(leaf), buffer_pool_manager_(buffer_pool_manager) {};


INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_) {
    UnlockAndUnPin();
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  return leaf_ == nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (++ index_ >= leaf_->GetSize()) {
    page_id_t next_id = leaf_->GetNextPageId();
    UnlockAndUnPin();
    if (next_id == INVALID_PAGE_ID) {
      leaf_ = nullptr;
    } else {
      auto *next_page = buffer_pool_manager_->FetchPage(next_id);
      next_page->RLatch();
      leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page->GetData());
      index_ = 0;
    }
  }
  return *this;
} 

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
