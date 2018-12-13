#include "buffer/buffer_pool_manager.h"

namespace cmudb
{

/*
 * BufferPoolManager Constructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(
    size_t pool_size,
    const std::string &db_file)
    : pool_size_(pool_size), disk_manager_{db_file}
{

  // A consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(100);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i)
  {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager()
{
  FlushAllPages();
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an entry
 * for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id)
{

  Page *page_ptr = nullptr;
  if (page_id == INVALID_PAGE_ID)
  {
    return nullptr;
  }

  // If page_id found in page_table
  // Pin the page and return
  //
  if (page_table_->Find(page_id, page_ptr))
  {
    page_ptr->pin_count_ += 1;
    return page_ptr;
  }

  // If page_id exists in page_table
  // and free list is not empty
  if (!free_list_->empty())
  {
    page_ptr = free_list_->front();
    free_list_->pop_front();
  }
  else
  {
    if (!replacer_->Victim(page_ptr))
    {
      return nullptr;
    }

    if (page_ptr->is_dirty_)
    {
      disk_manager_.WritePage(page_ptr->GetPageId(), page_ptr->GetData());
      page_ptr->is_dirty_ = false;
    }
    page_table_->Remove(page_ptr->GetPageId());
  }

  disk_manager_.ReadPage(page_id, page_ptr->data_);
  page_table_->Insert(page_id, page_ptr);
  page_ptr->page_id_ = page_id;
  page_ptr->pin_count_ += 1;
  return page_ptr;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to replacer
 * if pin_count<=0 before this call, return false.
 * is_dirty: set the dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty)
{

  Page *page_ptr = nullptr;
  if (!page_table_->Find(page_id, page_ptr))
  {
    return false;
  }

  if (page_ptr->pin_count_ <= 0)
  {
    return false;
  }

  page_ptr->pin_count_ -= 1;
  if (page_ptr->pin_count_ == 0)
  {
    replacer_->Insert(page_ptr);
  }

  if (is_dirty)
  {
    page_ptr->is_dirty_ = true;
  }

  return true;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id)
{
  if (page_id == INVALID_PAGE_ID)
  {
    return false;
  }

  Page *page_ptr = nullptr;
  if (!page_table_->Find(page_id, page_ptr))
  {
    return false;
  }

  disk_manager_.WritePage(page_id, page_ptr->GetData());
  return true;
}

/*
 * Used to flush all dirty pages in the buffer pool manager
 */
void BufferPoolManager::FlushAllPages()
{
  for (size_t i = 0; i < pool_size_; i++)
  {
    const Page &page = pages_[i];
    if (page.is_dirty_)
    {
      FlushPage(page.page_id_);
    }
  }
}

/**
 * User should call this method for deleting a page. This routine will call disk
 * manager to deallocate the page.
 * 
 * First, if page is found within page table, buffer pool manager should be
 * reponsible for removing this entry out of page table, reseting page metadata
 * and adding back to free list.
 * 
 * Second, call disk manager's DeallocatePage() method to delete from disk file.
 * 
 * If the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id)
{
  Page *page_ptr = nullptr;

  if (page_table_->Find(page_id, page_ptr))
  {
    if (page_ptr->GetPinCount() != 0)
    {
      return false;
    }

    page_table_->Remove(page_id);
    page_ptr->is_dirty_ = false;
    page_ptr->pin_count_ = 0;
    page_ptr->ResetMemory();
    disk_manager_.DeallocatePage(page_id);
  }
  return false;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either from
 * free list or lru replacer(NOTE: always choose from free list first), update
 * new page's metadata, zero out memory and add corresponding entry into page
 * table.
 * return nullptr if all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id)
{

  // return nullptr if all the pages in pool are pinned
  if (free_list_->empty() && replacer_->Size() == 0)
  {
    return nullptr;
  }

  Page *page_ptr = nullptr;
  page_id = disk_manager_.AllocatePage();
  if (!free_list_->empty())
  {
    page_ptr = free_list_->front();
    free_list_->pop_front();
  }
  else
  {
    if (!replacer_->Victim(page_ptr))
    {
      return nullptr;
    }

    if (page_ptr->is_dirty_)
    {
      disk_manager_.WritePage(page_ptr->GetPageId(), page_ptr->GetData());
      page_ptr->is_dirty_ = false;
    }
    page_table_->Remove(page_ptr->GetPageId());
  }

  page_ptr->ResetMemory();
  page_table_->Insert(page_id, page_ptr);
  page_ptr->page_id_ = page_id;
  page_ptr->pin_count_ += 1;
  return page_ptr;
}

} // namespace cmudb
