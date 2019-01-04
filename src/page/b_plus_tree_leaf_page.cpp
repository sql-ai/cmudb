/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb
{

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id)
{
  SetPageType(IndexPageType::LEAF_PAGE);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetSize(0);
  SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeLeafPage)) / sizeof(MappingType) - 1);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const
{
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id)
{
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const
{
  int start = 0;
  int end = GetSize();
  while (start < end)
  {
    int mid = (start + end) / 2;
    int compared = comparator(key, array[mid].first);
    if (compared <= 0)
    {
      end = mid;
    }
    else
    {
      start = mid + 1;
    }
  }
  return start;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const
{
  assert(index < GetSize());
  KeyType key = array[index].first;
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index)
{
  assert(index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator)
{
  //check if > k???
  int key_index = KeyIndex(key, comparator);
  for (int i = GetSize() - 1; i >= key_index; i--)
  {
    array[i + 1] = array[i];
  }
  array[key_index].first = key;
  array[key_index].second = value;
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient,
                                            BufferPoolManager *buffer_pool_manager)
{
  assert(recipient->GetSize() == 0);
  recipient->CopyHalfFrom(array, GetSize());

  SetSize(GetMinSize());
  recipient->next_page_id_ = next_page_id_;
  next_page_id_ = recipient->GetPageId();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size)
{
  assert(GetSize() == 0);
  SetSize(size - GetMinSize());
  for (int i = 0; i < size - GetMinSize(); i++)
  {
    array[i] = items[GetMinSize() + i];
  }
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const
{
  for (int i = 0; i < GetSize(); i++)
  {
    if (comparator(key, array[i].first) == 0)
    {
      value = array[i].second;
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator)
{
  int i = KeyIndex(key, comparator);
  assert(i > -1 && i < GetSize());
  if (comparator(key, array[i].first) == 0)
  {
    for (int j = i; j < GetSize(); j++)
    {
      array[j] = array[j + 1];
    }
    IncreaseSize(-1);
  }
  return GetSize();
} // namespace cmudb

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int index_in_parent,
                                           BufferPoolManager *buffer_pool_manager)
{
  recipient->CopyAllFrom(array, GetSize());
  SetSize(0);
  recipient->IncreaseSize(GetSize());
  recipient->SetNextPageId(next_page_id_);

  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());

  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_page =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);

  parent_page->Remove(index_in_parent);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size)
{
  int old_size = GetSize();
  IncreaseSize(size);
  for (int i = 0; i < size; i++)
  {
    array[old_size + i] = items[i];
  }
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
  recipient->CopyLastFrom(array[0]);
  for (int i = 0; i < GetSize() - 1; ++i)
    array[i] = array[i + 1];
  IncreaseSize(-1);
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());

  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_page =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);

  int index_in_parent = parent_page->ValueIndex(GetPageId());
  parent_page->SetKeyAt(index_in_parent, array[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item)
{
  array[GetSize()] = item;
  IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager)
{

  recipient->CopyFirstFrom(array[GetSize() - 1], parentIndex, buffer_pool_manager);
  IncreaseSize(-1);
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());

  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_page =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);

  int index_in_parent = parent_page->ValueIndex(GetPageId());
  parent_page->SetKeyAt(index_in_parent, array[GetSize() - 1].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager)
{
  for (int i = 0, size = GetSize(); i < size; i++)
  {
    array[i + 1] = array[i];
  }
  IncreaseSize(1);
  array[0] = item;
  Page *page = buffer_pool_manager->FetchPage(GetPageId());
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page);
  node->SetParentPageId(GetParentPageId());
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const
{
  if (GetSize() == 0)
  {
    return "";
  }
  std::ostringstream stream;
  if (verbose)
  {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end)
  {
    if (first)
    {
      first = false;
    }
    else
    {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose)
    {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                 GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                 GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                 GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                 GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                 GenericComparator<64>>;
} // namespace cmudb
