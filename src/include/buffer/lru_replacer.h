/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 * 
 * LRU rule is to throw out the block that has not been read or written for the
 * longest time. This method requires that the buffer manager maintain a table
 * indicating the last time the block in each buffer was accessed. 
 * 
 * It also requires that each database access make an entry in this table, 
 * so there is significant effort in maintaining this information.
 * 
 * However, LRU is an effective strategy;
 *  Intuitively, buffers that have not been used for a long time are less likely
 *  to be accessed sooner than those that have been accessed recently.
 * 
 * Insert(T): 
 *  Mark the element T as having been accessed in the database. 
 *  This means that the element is now the most frequently accessed and 
 *  should not be selected as the victim for removal from the buffer pool 
 *  (assuming there exists more than one element).
 * 
 * Victim(T): 
 *  Remove the object that was accessed the least recently compared to all 
 *  the elements being tracked by the Replacer, 
 *  store its contents in the value T, and then return true.
 * 
 *  If there is only one element in the Replacer, then that is always considered 
 *  to be the least recently used. If there are zero elements in the Replacer, 
 *  then this function should return false.
 * 
 * Erase(T): 
 *  Completely remove the element T from the Replacer's internal tracking data structure
 *  regardless of where it appears in the LRU replacer.
 * 
 *  This should delete all tracking data from the element. 
 *  If the element T exists and it was removed, then the function should return true.
 *  Otherwise, return false.
 * 
 * Size():
 *  Return the number of elements that this Replacer is tracking.
 */

#pragma once

#include <list>
#include <mutex>
#include <unordered_map>

#include "buffer/replacer.h"
#include "hash/extendible_hash.h"

namespace cmudb
{

template <typename T>
class LRUReplacer : public Replacer<T>
{
public:
  // do not change public interface
  LRUReplacer();

  ~LRUReplacer();

  void Insert(const T &value);

  bool Victim(T &value);

  bool Erase(const T &value);

  size_t Size();

private:
  std::mutex mu_;
  std::list<T> list_;
  std::unordered_map<T, typename std::list<T>::iterator> map_;
};

} // namespace cmudb
