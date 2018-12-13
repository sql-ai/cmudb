/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include "hash/hash_table.h"
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

using namespace std;

namespace cmudb
{

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V>
{
public:
  struct Block
  {
    int nub;
    map<K, V> records;
    Block() : nub(0)
    {
      records = map<K, V>();
    }
    Block(int j) : Block()
    {
      nub = j;
    }
    void Clear()
    {
      nub = 0;
      records.clear();
    }
  };

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
  vector<shared_ptr<Block>> buckets_;
  int global_depth_;
  size_t size_;
  mutex mu_;
  void SplitBlock(shared_ptr<Block> &block);
};
} // namespace cmudb
