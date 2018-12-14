#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"
#include <functional>
#include <algorithm>
#include <cmath>
#include <memory>

namespace cmudb
{
  using namespace std;

  /*
   * constructor
   * array_size: fixed array size for each bucket
   */
  template <typename K, typename V>
  ExtendibleHash<K, V>::ExtendibleHash(size_t size) : global_depth_(0), bucket_size_(size) {
    hashtable_.push_back(make_shared<Bucket>());
  }

  /*
   * helper function to calculate the hashing address of input key
   */
  template <typename K, typename V>
  size_t ExtendibleHash<K, V>::HashKey(const K &key) {
    size_t hash_num = hash<K>{}(key);
    return hash_num & ((1 << global_depth_) - 1);
  }

  template <typename K, typename V>
  bool ExtendibleHash<K, V>::IsIBitOn(int n, int i) {
    return (1UL & (n >> i));
  }

  /*
   * helper function to return global depth of hash table
   * NOTE: you must implement this function in order to pass test
   */
  template <typename K, typename V>
  int ExtendibleHash<K, V>::GetGlobalDepth() const {
    return global_depth_;
  }

  /*
   * helper function to return local depth of one specific bucket
   * NOTE: you must implement this function in order to pass test
   */
  template <typename K, typename V>
  int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
    return hashtable_[bucket_id]->local_depth_;
  }

  /*
   * helper function to return current number of bucket in hash table
   */
  template <typename K, typename V>
  int ExtendibleHash<K, V>::GetNumBuckets() const {
    return hashtable_.size();
  }

  /*
   * lookup function to find value associate with input key
   */
  template <typename K, typename V>
  bool ExtendibleHash<K, V>::Find(const K &key, V &value)
  {
    const auto& blocks = hashtable_[HashKey(key)]->items_;
    auto itr = find_if(blocks.begin(), blocks.end(), [key](const auto& p) {
      return p.first == key;
    });
    if (itr != blocks.end()) {
      value = itr->second;
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
    auto& blocks = hashtable_[HashKey(key)]->items_;
    auto itr = find_if(blocks.begin(), blocks.end(), [key](const auto& p) {
      return p.first == key;
    });
    if (itr != blocks.end()) {
      blocks.erase(itr);
      return true;
    }
    return false;
  }

  /*
   * insert <key,value> entry in hash table
   * Split & Redistribute bucket when there is overflow and if necessary increase
   * global depth
   */
  template <typename K, typename V>
  void ExtendibleHash<K, V>::Insert(const K &key, const V &value)
  {
    lock_guard<mutex> lock(mu_);
    size_t bucket_id = HashKey(key);
    shared_ptr<Bucket> bucket = hashtable_[bucket_id];    

    auto itr = find_if(bucket->items_.begin(), bucket->items_.end(), [key](const auto& p) {
      return p.first == key;
    });
    if (itr != bucket->items_.end()) {
      itr->second = value;
      return;
    };

    hashtable_[bucket_id]->items_.push_back(make_pair(key, value));
    if (bucket->items_.size() > bucket_size_) {
      SplitBlocks(bucket_id);
    }
  }

  template <typename K, typename V>
  void ExtendibleHash<K, V>::SplitBlocks(size_t id)
  {
    shared_ptr<Bucket> bucket = hashtable_[id];
    if (bucket->local_depth_ == global_depth_) {
      global_depth_++;
      hashtable_.resize((1UL << global_depth_));
      size_t num_buckets = GetNumBuckets();
      size_t start_update_bucket = 1UL << (global_depth_ - 1);
      for (size_t i = start_update_bucket; i < num_buckets; i++) {
        size_t j = (i ^ start_update_bucket);
        hashtable_[i] = hashtable_[j];
      }
      SplitBlocks(id);
    }
    else {
      vector<pair<K, V>> all = bucket->items_;
      hashtable_[id]->items_.clear();
      shared_ptr<Bucket> b1 = make_shared<Bucket>();
      shared_ptr<Bucket> b2 = make_shared<Bucket>();

      for (auto item : all) {
        size_t hash_num = hash<K>{} (item.first);
        if (IsIBitOn(hash_num, bucket->local_depth_)) {
          b2->items_.push_back(item);
        }
        else {
          b1->items_.push_back(item);
        }
      }

      b1->local_depth_ = bucket->local_depth_ + 1;
      b2->local_depth_ = bucket->local_depth_ + 1;

      size_t new_id;
      for (auto it = b1->items_.begin(); it != b1->items_.end(); ++it) {
        size_t bucket_id = HashKey(it->first);
        if (bucket_id != id) new_id = bucket_id;
        hashtable_[bucket_id] = b1;
      }

      for (auto it = b2->items_.begin(); it != b2->items_.end(); ++it) {
        size_t bucket_id = HashKey(it->first);
        if (bucket_id != id) new_id = bucket_id;
        hashtable_[bucket_id] = b2;
      }

      if (hashtable_[id]->items_.size() > bucket_size_) {
        SplitBlocks(id);
      } else if (hashtable_[new_id]->items_.size() > bucket_size_) {
        SplitBlocks(new_id);
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
