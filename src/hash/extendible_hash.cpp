#include "hash/extendible_hash.h"
#include "page/page.h"
#include <algorithm>
#include <functional>
#include <list>

using namespace std;

namespace cmudb
{

/*  
 * Check if kth bit of x is 1.
 */
bool IS_KBIT_ON(size_t x, size_t k)
{
  return (1UL & (x >> k));
}

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size)
{
  size_ = size;
  global_depth_ = 0;
  buckets_.resize(1);
  buckets_[0] = make_shared<Block>(0);
}

/*
 * helper function to calculate the hashing address of input key
 * For the given key, return the offset of the Bucket where it should be stored.
*/
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key)
{
  size_t h = hash<K>()(key);
  return (h & (GetNumBuckets() - 1));
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const
{
  return global_depth_;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const
{
  return buckets_[bucket_id]->nub;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const
{
  return (1UL << global_depth_);
}

/*
 * Lookup function to find value associate with input key.
 * For the given key K, check to see whether it exists in the hash table. 
 * If it does, store the pointer to its corresponding value in V and return true. 
 * If the key does not exist, then return false.
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value)
{
  size_t bucket_id = HashKey(key);
  shared_ptr<Block> b = buckets_[bucket_id];
  auto it = b->records.find(key);
  if (it != b->records.end())
  {
    value = it->second;
    return true;
  }

  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key)
{
  lock_guard<mutex> lock(mu_);
  size_t bucket_id = HashKey(key);
  shared_ptr<Block> b = buckets_[bucket_id];
  auto it = b->records.find(key);
  if (it != b->records.end())
  {
    b->records.erase(key);
    return true;
  }

  return false;
}

/*
 * Insert <key,value> entry in hash table.
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth.
 * If the key K already exists, overwrite its value with the new value V.
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value)
{
  lock_guard<mutex> lock(mu_);
  size_t bucket_id = HashKey(key);
  shared_ptr<Block> block = buckets_[bucket_id];

  // If key exists, overwrite its value with the new value.
  auto it = block->records.find(key);
  if (it != block->records.end())
  {
    block->records[key] = value;
    return;
  }

  block->records[key] = value;
  if (block->records.size() <= size_)
  {
    return;
  }

  SplitBlock(block);
}

/*
 * SplitBlock into 2 blocks.
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::SplitBlock(shared_ptr<Block> &block)
{
  if (block->nub == global_depth_)
  {
    global_depth_ += 1;

    size_t num_buckets = GetNumBuckets();
    buckets_.resize(num_buckets);

    for (size_t i = (1UL << (global_depth_ - 1)); i < num_buckets; i++)
    {
      size_t j = (i ^ (1UL << (global_depth_ - 1)));
      buckets_[i] = buckets_[j];
    }
    SplitBlock(block);
  }
  else
  {
    size_t j = static_cast<size_t>(block->nub);
    block->nub += 1;
    shared_ptr<Block> b1 = make_shared<Block>(block->nub);
    shared_ptr<Block> b2 = make_shared<Block>(block->nub);

    copy_if(block->records.begin(), block->records.end(),
            inserter(b1->records, b1->records.end()),
            [this, j](pair<K, V> it) { return IS_KBIT_ON(this->HashKey(it.first), j); });

    remove_copy_if(block->records.begin(), block->records.end(),
                   inserter(b2->records, b2->records.end()),
                   [this, j](pair<K, V> it) { return IS_KBIT_ON(this->HashKey(it.first), j); });

    block->Clear();

    block = b1;
    if (block->records.size() > size_ || b2->records.size() > size_)
    {
      if (b2->records.size() > size_)
      {
        block = b2;
      }
      SplitBlock(block);
    }
    else
    {
      for (auto it = block->records.begin(); it != block->records.end(); ++it)
      {
        buckets_[HashKey(it->first)] = block;
      }
      for (auto it = b2->records.begin(); it != b2->records.end(); ++it)
      {
        buckets_[HashKey(it->first)] = b2;
      }
    }
  }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, string>;
template class ExtendibleHash<int, list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
