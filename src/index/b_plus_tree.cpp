/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
thread_local int BPlusTree<KeyType, ValueType, KeyComparator>::root_locked_cnt = 0;

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
  // 1. find the page
  B_PLUS_TREE_LEAF_PAGE_TYPE *res_page = FindLeafPage(key, false, OpType::READ, transaction);
  if (!res_page) return false;
  // 2. find the record
  result.resize(1);
  bool ret = res_page->Lookup(key, result[0], comparator_);
  // 3. Unpin the page
  RemovePagesInTransaction(LockType::SHARED, transaction, res_page->GetPageId());
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  LockRootPage(LockType::EXCLUSIVE);
  if (IsEmpty()) {
    StartNewTree(key, value);
    UnlockRootPage(LockType::EXCLUSIVE);
    return true;
  }
  UnlockRootPage(LockType::EXCLUSIVE);
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // 1. create root page
  page_id_t new_page_id;
  auto root_page = buffer_pool_manager_->NewPage(new_page_id);
  if (!root_page)  throw "out of memory";

  B_PLUS_TREE_LEAF_PAGE_TYPE *root_tree_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(root_page->GetData());
  root_tree_page->Init(new_page_id, INVALID_PAGE_ID);
  root_page_id_ = new_page_id;
  UpdateRootPageId(true);
  // 2. insert record in root
  root_tree_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
  // 1. find the key
  B_PLUS_TREE_LEAF_PAGE_TYPE *insert_page = FindLeafPage(key, false, OpType::INSERT, transaction);
  ValueType tmp_val;
  if (insert_page->Lookup(key, tmp_val, comparator_)) {
    RemovePagesInTransaction(LockType::EXCLUSIVE, transaction);
    return false;
  }
  // 2. insert new record
  insert_page->Insert(key, value, comparator_);
  if (insert_page->GetSize() > insert_page->GetMaxSize()) {
    // 2.1 split
    B_PLUS_TREE_LEAF_PAGE_TYPE *split_page = Split(insert_page, transaction);
    InsertIntoParent(insert_page, split_page->KeyAt(0), split_page, transaction);
  }
  // 2.2 insert successfully
  RemovePagesInTransaction(LockType::EXCLUSIVE, transaction);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) {
  // 1. create new page
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (!new_page) throw "out of memory";
  // 2. move half of records from input page to new page
  new_page->WLatch();
  transaction->AddIntoPageSet(new_page);
  N *new_tree_page = reinterpret_cast<N *>(new_page->GetData());
  new_tree_page->Init(new_page_id, node->GetParentPageId());
  node->MoveHalfTo(new_tree_page, buffer_pool_manager_);
  return new_tree_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // 1. handle split of root page
  if (old_node->IsRootPage()) {
    // 1.1 create new root page
    auto new_page = buffer_pool_manager_->NewPage(root_page_id_);
    B_PLUS_TREE_INTERNAL_PAGE *new_root_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(new_page->GetData());
    new_root_page->Init(root_page_id_);
    // 1.2 set two records of new root page
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId();
    // buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return;
  }
  // 2. handle split of internal page
  page_id_t parent_id = old_node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_id);
  B_PLUS_TREE_INTERNAL_PAGE *parent_tree_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(parent_page->GetData());
  new_node->SetParentPageId(parent_id);
  // buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  parent_tree_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // 3.split recursively
  if (parent_tree_page->GetSize() > parent_tree_page->GetMaxSize()) {
    B_PLUS_TREE_INTERNAL_PAGE *split_page = Split(parent_tree_page, transaction);
    InsertIntoParent(parent_tree_page, split_page->KeyAt(0), split_page, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty())
    return;
  // 1. get the page
  B_PLUS_TREE_LEAF_PAGE_TYPE *delete_page = FindLeafPage(key, false, OpType::DELETE, transaction);
  // 2. delete the record
  int after_sz = delete_page->RemoveAndDeleteRecord(key, comparator_);
  // 3. merge or redistribute if size < min size
  if (after_sz < delete_page->GetMinSize())
    CoalesceOrRedistribute(delete_page, transaction);
  RemovePagesInTransaction(LockType::EXCLUSIVE, transaction);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // 1. handle root page
  if (node->IsRootPage()) {
    bool delete_root = AdjustRoot(node);
    if (delete_root)
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    return delete_root;
  }
  // 2. get sibling of node
  N *sibling;
  bool node_left = FindSibling(node, sibling, transaction);
#ifdef DBG
    // LOG_DEBUG("node - %d\tsibling - %d\n", node->GetPageId(), sibling->GetPageId());
#endif
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE *parent_tree_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(parent_page->GetData());
  // 3. redistribute condition
  if (node->GetSize() + sibling->GetSize() > node->GetMaxSize()) {
    int node_idx = parent_tree_page->ValueIndex(node->GetPageId());
    Redistribute(sibling, node, node_idx);
    buffer_pool_manager_->UnpinPage(parent_tree_page->GetPageId(), false);
    return false;
  }
  // 4. merge condition
  if (node_left)  std::swap(node, sibling);  // let node is after sibling
  int remove_idx = parent_tree_page->ValueIndex(node->GetPageId());
  Coalesce(sibling, node, parent_tree_page, remove_idx, transaction);
  buffer_pool_manager_->UnpinPage(parent_tree_page->GetPageId(), true);
  return true;
}

/*
 * Find the left sibling of the node firstly,
 * if the node is the left most page, find the right sibling of the node.
 * @return: true means node is the left most page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::FindSibling(N *node, N * &sibling, Transaction *transaction) {
  // 1. get parent page
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE *parent_tree_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(parent_page->GetData());
  // 2. get sibling page (left or right)
  int idx = parent_tree_page->ValueIndex(node->GetPageId());
  int sibling_idx = (idx > 0) ? (idx - 1) : (idx + 1);
  page_id_t sibling_page_id = parent_tree_page->ValueAt(sibling_idx);
#ifdef DBG
    LOG_DEBUG("node - %d\tsibling - %d\n", node->GetPageId(), sibling_page_id);
    for (Page *page : *transaction->GetPageSet()) {
      page_id_t id = page->GetPageId();
      LOG_DEBUG("%d ", id);
    }
    LOG_DEBUG("\n");
#endif
  BPlusTreePage *sibling_page = ConcurrentFetchPage(sibling_page_id, OpType::DELETE, INVALID_PAGE_ID, transaction);
  sibling = reinterpret_cast<N *>(sibling_page);
  buffer_pool_manager_->UnpinPage(parent_tree_page->GetPageId(), false);
#ifdef DBG
    // LOG_DEBUG("node - %d\tsibling - %d\n", node->GetPageId(), sibling->GetPageId());
#endif
  return idx == 0;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
  // 1. move all record from node to neighbor_node
  node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
  // 2. delete node page
  transaction->AddIntoDeletedPageSet(node->GetPageId());
  // 3. parent delete the record of node recursively
  parent->Remove(index);
  if (parent->GetSize() <= parent->GetMinSize()) 
    return CoalesceOrRedistribute(parent, transaction);
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // 1. node is the left most, neighbor_node is after node
  if (index == 0)
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  // 2. neighbor_node is before node
  else 
    neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // 1. when delete the last element in whole b+ tree
  if (old_root_node->IsLeafPage()) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  }
  // 2. when delete the last element in root page, but root page still has one child
  if (old_root_node->GetSize() == 1) {
    B_PLUS_TREE_INTERNAL_PAGE *root_tree_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(old_root_node);
    // 2.1. let the only child be root
    page_id_t new_root_id = root_tree_page->RemoveAndReturnOnlyChild();
    root_page_id_ = new_root_id;
    UpdateRootPageId();
    auto *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    B_PLUS_TREE_INTERNAL_PAGE *new_root_tree_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(new_root_page->GetData());
    new_root_tree_page->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  // 3. do not delete the old root page
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType tmp_key;
  // find the left most leaf page
  B_PLUS_TREE_LEAF_PAGE_TYPE *start_leaf = FindLeafPage(tmp_key, true);
  UnlockRootPage(LockType::SHARED);
  return INDEXITERATOR_TYPE(0, start_leaf, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  B_PLUS_TREE_LEAF_PAGE_TYPE *start_leaf = FindLeafPage(key, false);
  UnlockRootPage(LockType::SHARED);
  if (!start_leaf)
    return INDEXITERATOR_TYPE(0, start_leaf, buffer_pool_manager_);
  return INDEXITERATOR_TYPE(start_leaf->KeyIndex(key, comparator_), start_leaf, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, 
                                                          OpType op, Transaction *transaction) {
  LockType lock_type = (op == OpType::READ) ? LockType::SHARED : LockType::EXCLUSIVE;
  LockRootPage(lock_type);
  if (IsEmpty()) {
    UnlockRootPage(lock_type);
    return nullptr;
  }
  BPlusTreePage *tree_page = ConcurrentFetchPage(root_page_id_, op, INVALID_PAGE_ID, transaction);
  page_id_t next_id;
  page_id_t ptr_id = root_page_id_;
  while (!tree_page->IsLeafPage()) {
    B_PLUS_TREE_INTERNAL_PAGE *internal_tree_page = static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(tree_page);
    if (leftMost) 
      next_id = internal_tree_page->ValueAt(0);
    else          
      next_id = internal_tree_page->Lookup(key, comparator_);

    tree_page = ConcurrentFetchPage(next_id, op, ptr_id, transaction);
    ptr_id = next_id;
  }
  return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(tree_page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * Unlock, unpin pages in transaction
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemovePagesInTransaction(LockType lock_type, Transaction *transaction, page_id_t cur_id) {
  UnlockRootPage(lock_type);
  // 1. if transaction is null, unlock and unpin the current page
  if (!transaction) {
    UnlockPage(lock_type, cur_id);  
    buffer_pool_manager_->UnpinPage(cur_id, false);
    return;
  }
  // 2. unlock and unpin all pages in transaction
  for (Page *page : *transaction->GetPageSet()) {
    page_id_t page_id = page->GetPageId();
    UnlockPage(lock_type, page);
    buffer_pool_manager_->UnpinPage(page_id, lock_type == LockType::EXCLUSIVE);
    // delete the page in DeletedPageSet
    if (transaction->GetDeletedPageSet()->find(page_id) != transaction->GetDeletedPageSet()->end()) {
      buffer_pool_manager_->DeletePage(page_id);
      transaction->GetDeletedPageSet()->erase(page_id);
    }
  }
  transaction->GetPageSet()->clear();
}

/*
 * Fetch page in concurrent environment
 */ 
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::ConcurrentFetchPage(page_id_t page_id, OpType op, page_id_t previous_id, Transaction *transaction) {
  LockType lock_type = (op == OpType::READ) ? LockType::SHARED : LockType::EXCLUSIVE;
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  LockPage(lock_type, page);
  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage  *>(page->GetData());
  // if the op is read or the page is safe, release the latch of previous page
  if (previous_id > 0 && (op == OpType::READ || tree_page->IsSafe(op)))
    RemovePagesInTransaction(lock_type, transaction, previous_id);
  if (transaction != nullptr)  
    transaction->AddIntoPageSet(page);  // add pages which are locked in set
  return tree_page;
}


/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
