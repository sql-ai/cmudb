/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb
{

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(
    BufferPoolManager &buffer_pool_manager,
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page,
    int position)
    : buffer_pool_manager_(buffer_pool_manager),
      leaf_page_(leaf_page),
      position_(position),
      is_end_(false)
{
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
