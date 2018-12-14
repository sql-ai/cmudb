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
#include <iostream>
#include <memory>
#include <mutex>
#include "hash/hash_table.h"

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
  
  bool IsIBitOn(int n, int i);
  void SplitBlocks(size_t id);
  
  struct Bucket
  {
    int local_depth_;
    std::vector<std::pair<K, V>> items_;
    
    Bucket() 
    {
      local_depth_ = 0;
      items_.clear();
    }
    Bucket(const Bucket& b)
    {
      items_ = b.items_;
      local_depth_ = b.local_depth_;
    }
    Bucket& operator=(const Bucket& other)
    {
      if (&other == this)
          return *this;
      local_depth_ = other.local_depth_;
      items_ = other.items_;
      return *this;
    }
  };
  
  // add your own member variables here
  int global_depth_;
  size_t bucket_size_;
  std::mutex mu_;
  std::vector<std::shared_ptr<Bucket>> hashtable_;
};
} // namespace cmudb
