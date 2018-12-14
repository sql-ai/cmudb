/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"
#include <algorithm>

namespace cmudb
{

template <typename T>
LRUReplacer<T>::LRUReplacer() {}

template <typename T>
LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T>
void LRUReplacer<T>::Insert(const T &value)
{
  std::lock_guard<std::mutex> lock(mu_);
  if (table_.find(value) != table_.end())
  {
    replacer_.erase(table_[value]);
    table_.erase(value);
  }
  replacer_.push_front(value);
  table_.emplace(std::make_pair(value, replacer_.begin()));
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T>
bool LRUReplacer<T>::Victim(T &value)
{
  std::lock_guard<std::mutex> lock(mu_);
  if (!replacer_.empty())
  {
    value = replacer_.back();
    replacer_.pop_back();
    table_.erase(value);
    return true;
  }
  return false;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T>
bool LRUReplacer<T>::Erase(const T &value)
{
  std::lock_guard<std::mutex> lock(mu_);
  if (table_.find(value) != table_.end())
  {
    replacer_.erase(table_[value]);
    table_.erase(value);
    return true;
  }
  return false;
}

template <typename T>
size_t LRUReplacer<T>::Size()
{
  return replacer_.size();
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
