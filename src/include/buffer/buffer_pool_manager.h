/*
 * buffer_pool_manager.h
 *
 * Functionality: The simplified Buffer Manager interface allows a client to
 * new/delete pages on disk, to read a disk page into the buffer pool and pin
 * it, also to unpin a page in the buffer pool.
 * 
 * BufferPoolManager uses the ExtendibleHashTable and LRUReplacer classes.
 * It uses the ExtendibleHashTable to map page_id's to Page objects. 
 * 
 * It uses the LRUReplacer to keep track of when Page objects are accessed
 * so that it can decide which one to evict when it must free a frame 
 * to make room for copying a new physical page from disk.
 * 
 * FetchPage(page_id): 
 *  This returns a Page object that contains contents of the given page_id.
 *  The function first checks its internal page table to see whether there
 *    already exists a Page that is mapped to the page_id. 
 *    If it does, then it returns it. 
 *    Otherwise it will retrieve the physical page from the DiskManager.
 * 
 *  To do this, the function selects a Page object to store the physical page's contents.
 *    If there are free frames in the page table, then the function will select a random one to use.
 *    Otherwise, it will use the LRUReplacer to select an unpinned Page that was 
 *    least recently used as the "victim" page. 
 * 
 *    If there are no free slots (i.e., all the pages are pinned), 
 *      then return a null pointer (nullptr).
 * 
 *  If the selected victim page is dirty, then use the DiskManager to write 
 *    its contents out to disk. Then use DiskManager to read the target physical 
 *    page from disk and copy its contents into that Page object.
 * 
 *  IMPORTANT: FetchPage(page_id) must mark the Page as pinned and remove
 *    its entry from LRUReplacer before it is returned to the caller.
 * 
 * NewPage(page_id):
 *  Allocate a new physical page in the DiskManager, store the new page id 
 *  in the given page_id and store the new page in the buffer pool.
 * 
 *  This should have the same functionality as FetchPage() in terms of selecting 
 *  a victim page from LRUReplacer and initializing the Page's internal meta-data 
 *  (including incrementing the pin count).
 * 
 * UnpinPage(page_id, is_dirty):
 *  Decrement the pin counter for the Page specified by the given page_id. 
 *  If the pin counter is zero, then fn will add the Page object into the LRUReplacer. 
 *  If the given is_dirty is true, then mark the Page as dirty; 
 *    otherwise, leave the Page's dirty flag unmodified. 
 *  If there is no entry in the page table for the given page_id, then return false.
 * 
 * FlushPage(page_id):
 *  This will retrieve the Page object specified by the given page_id and then 
 *  use the DiskManager to write its contents out to disk.
 * 
 *  Upon successful completion of that write operation, the function will return true.
 *  This function should not remove the Page from the buffer pool.
 *  It also does not need to update the LRUReplacer for the Page.
 *  If there is no entry in the page table for the given page_id, then return false.
 * 
 * FlushAllPages(): 
 *  For each Page object in the buffer pool, use the DiskManager to write their contens 
 *  out to disk. This function should not remove the Page from the buffer pool. 
 *  It also does not need to update the LRUReplacer for the Page.
 * 
 * DeletePage(page_id):
 *  Instruct the DiskManager to deallocate the physical page identified by the given page_id.
 *  You can only delete a page if it is currently unpinned.
 */

#pragma once
#include <list>
#include <mutex>

#include "buffer/lru_replacer.h"
#include "disk/disk_manager.h"
#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb
{
class BufferPoolManager
{
public:
  BufferPoolManager(size_t pool_size,
                    const std::string &db_file);

  ~BufferPoolManager();

  Page *FetchPage(page_id_t page_id);

  bool UnpinPage(page_id_t page_id, bool is_dirty);

  bool FlushPage(page_id_t page_id);

  Page *NewPage(page_id_t &page_id);

  bool DeletePage(page_id_t page_id);

private:
  size_t pool_size_;
  // array of pages
  Page *pages_;
  DiskManager disk_manager_;
  HashTable<page_id_t, Page *> *page_table_;
  Replacer<Page *> *replacer_;
  std::list<Page *> *free_list_;
  std::mutex latch_;

  void PinPage(Page *p)
  {
    p->pin_count_++;
  }
};

} // namespace cmudb
