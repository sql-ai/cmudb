/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

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
  lock_guard<mutex> guard(mu_);
  if (map_.find(value) != map_.end())
  {
    list_.erase(map_[value]);
  }
  list_.push_front(value);
  map_[value] = list_.begin();
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T>
bool LRUReplacer<T>::Victim(T &value)
{
  lock_guard<mutex> guard(mu_);
  if (list_.empty())
    return false;

  value = list_.back();
  map_.erase(value);
  list_.pop_back();
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T>
bool LRUReplacer<T>::Erase(const T &value)
{
  lock_guard<mutex> guard(mu_);
  if (map_.find(value) == map_.end())
  {
    return false;
  }
  list_.erase(map_[value]);
  map_.erase(value);
  return true;
}

template <typename T>
size_t LRUReplacer<T>::Size()
{
  lock_guard<mutex> guard(mu_);
  return map_.size();
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
