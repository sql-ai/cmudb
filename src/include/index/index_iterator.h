/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb
{

#define INDEXITERATOR_TYPE \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator
{
public:
  // you may define your own constructor based on your member variables

  IndexIterator(
      BufferPoolManager &buffer_pool_manager,
      B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page,
      int position);

  bool isEnd()
  {
    return is_end_;
  }

  const MappingType &operator*()
  {
    return leaf_page_->GetItem(position_);
  }

  IndexIterator &operator++()
  {
    position_++;
    if (position_ >= leaf_page_->GetSize())
    {
      page_id_t next = leaf_page_->GetNextPageId();
      if (next == INVALID_PAGE_ID)
      {
        is_end_ = true;
      }
      else
      {
        position_ = 0;
        buffer_pool_manager_.UnpinPage(leaf_page_->GetPageId(), false);
        Page *page = buffer_pool_manager_.FetchPage(next);
        leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);
      }
    }
    return *this;
  }

private:
  BufferPoolManager &buffer_pool_manager_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_;
  int position_;
  bool is_end_;
};

} // namespace cmudb
