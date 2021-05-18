#include "buffer/buffer_pool_manager.h"

namespace cmudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 DiskManager *disk_manager,
                                                 LogManager *log_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager),
          log_manager_(log_manager) {
    // a consecutive memory space for buffer pool
    pages_ = new Page[pool_size_];
    page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
    replacer_ = new LRUReplacer<Page *>;
    free_list_ = new std::list<Page *>;

    // put all the pages into free list
    for (size_t i = 0; i < pool_size_; ++ i) {
        free_list_->push_back(&pages_[i]);
    }
#ifdef DBG
    LOG_DEBUG("Constructor\n");
#endif
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
    delete[] pages_;
    delete page_table_;
    delete replacer_;
    delete free_list_;
}

/*
 * This function returns a Page object that contains the contents of
 * the given page_id
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
    std::unique_lock<std::mutex> lock(latch_);
#ifdef DBG
    LOG_DEBUG("Fetch Page - %d\n", page_id);
#endif
    Page *page = nullptr;
    if (page_table_->Find(page_id, page)) {
        ++ page->pin_count_;
        replacer_->Erase(page); // because the page is pinned
        return page;
    }
    page = GetFreePage();
    if (page == nullptr)
        return nullptr;
    if (page->is_dirty_)
        FlushPage(page->page_id_);   // flush old page
    page_table_->Remove(page->page_id_);
    page_table_->Insert(page_id, page);
    disk_manager_->ReadPage(page_id, page->data_);
    page->page_id_ = page_id;
    page->is_dirty_ = false;
    page->pin_count_ = 1;
    return page;
}

/*
 * Get free page from free_list or LRU replacer
 */
Page *BufferPoolManager::GetFreePage() {
    Page *page = nullptr;
    if (free_list_->empty() && replacer_->Size() == 0)
        return page;
    if (!free_list_->empty()) {
        page = free_list_->front();
        free_list_->pop_front();
    }
    else if (!replacer_->Victim(page)){
        return nullptr;
    }
    return page;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    std::unique_lock<std::mutex> lock(latch_);
#ifdef DBG
    LOG_DEBUG("Unpin Page - %d\n", page_id);
#endif
    Page *page = nullptr;
    page_table_->Find(page_id, page);
    if (page == nullptr)
        return false;
    page->is_dirty_ |= is_dirty;
    if (page->GetPinCount() <= 0)
        return false;
    if (-- page->pin_count_ == 0)
        replacer_->Insert(page);
    return true;
}

/*
 * Used to flush a particular page of the buffer pool to disk.
 * Should call the write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
    Page *page = nullptr;
    page_table_->Find(page_id, page);
    if (page == nullptr || page->page_id_ == INVALID_PAGE_ID)
        return false;
    disk_manager_->WritePage(page_id, page->data_);
    page->is_dirty_ = false;
    return true;
}

void BufferPoolManager::FlushAllPages() {
    std::unique_lock<std::mutex> lock(latch_);
    for (uint16_t i = 0; i < pool_size_; ++ i) {
        if (pages_[i].pin_count_ == 0 && pages_[i].is_dirty_) {
            disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
            pages_[i].is_dirty_ = false;
        }
    }
}

/*
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
    std::unique_lock<std::mutex> lock(latch_);
    Page *page = nullptr;
    page_table_->Find(page_id, page);
    if (page != nullptr) {
        if (page->pin_count_ > 0)
            return false;
        page_table_->Remove(page_id);
        replacer_->Erase(page);
        page->page_id_ = INVALID_PAGE_ID;
        page->is_dirty_ = false;
        page->ResetMemory();
        free_list_->push_back(page);
    }
    disk_manager_->DeallocatePage(page_id);
    return true;
}

/*
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
    std::unique_lock<std::mutex> lock(latch_);
    Page *page = nullptr;
    page = GetFreePage();
    if (page == nullptr)
        return nullptr;
    page_id = disk_manager_->AllocatePage();

#ifdef DBG
    LOG_DEBUG("New Page - %d\n", page_id);
#endif

    if (page->is_dirty_)
        FlushPage(page->page_id_);
    page_table_->Remove(page->page_id_);
    page_table_->Insert(page_id, page);

    page->page_id_ = page_id;
    page->ResetMemory();
    page->is_dirty_ = false;
    page->pin_count_ = 1;
    return page;
}
} // namespace cmudb
