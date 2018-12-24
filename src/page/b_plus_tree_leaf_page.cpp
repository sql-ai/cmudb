/**
 * b_plus_tree_leaf_page.cpp
 * 
 * #define B_PLUS_TREE_LEAF_PAGE_TYPE \
 *  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
 *
 * #define B_PLUS_TREE_PARENT_PAGE_TYPE \
 *  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>
 *
 * #define MappingType std::pair<KeyType, ValueType>
 *
 * #define INDEX_TEMPLATE_ARGUMENTS \
 *  template <typename KeyType, typename ValueType, typename KeyComparator>
 */

#include "page/b_plus_tree_leaf_page.h"
#include "common/exception.h"
#include "common/rid.h"
#include <sstream>

namespace cmudb
{

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page.
 * Including set page type,
 *  set current size to zero, 
 *  set page id/parent id, set
 *  next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id)
{
  SetPageId(page_id);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetPreviousPageId(INVALID_PAGE_ID);
  size_t leaf_page_size = sizeof(BPlusTreeLeafPage);
  size_t max_size = (PAGE_SIZE - leaf_page_size) / sizeof(MappingType) - 1;
  SetMaxSize(max_size);
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

INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetPreviousPageId() const
{
  return prev_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetPreviousPageId(
    page_id_t prev_page_id)
{
  prev_page_id_ = prev_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key,
    const KeyComparator &comparator) const
{
  int begin = 0;
  int end = GetSize();
  while (begin < end)
  {
    int mid = begin + (end - begin) / 2;
    int cmp = comparator(key, array[mid].first);
    if (cmp <= 0)
    {
      end = mid;
    }
    else
    {
      begin = mid + 1;
    }
  }

  return begin;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const
{
  // replace with your own code
  //
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index)
{
  // replace with your own code
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion.
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(
    const KeyType &key,
    const ValueType &value,
    const KeyComparator &comparator)
{
  int idx = KeyIndex(key, comparator);

  if (idx == GetSize() && comparator(key, KeyAt(idx)) == 0)
  {
    return GetSize();
  }

  int i = GetSize() - 1;
  while (i >= 0)
  {
    if (comparator(key, array[i].first) < 0)
    {
      array[i + 1] = array[i];
    }
    else
    {
      break;
    }
    i -= 1;
  }

  array[i + 1].first = key;
  array[i + 1].second = value;
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
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager)
{
  recipient->SetNextPageId(GetNextPageId());
  next_page_id_ = recipient->GetPageId();
  recipient->SetPreviousPageId(GetPageId());
  int mx_size = GetMaxSize();
  int siz = mx_size / 2;
  int j = 0;
  for (int i = siz; i < GetSize(); i++)
  {
    recipient->array[j] = array[i];
    j++;
  }
  SetSize(siz);
  recipient->SetSize(j);
}

// Empty for now
//
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size)
{
  return;
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
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(
    const KeyType &key,
    ValueType &value,
    const KeyComparator &comparator) const
{
  int idx = KeyIndex(key, comparator);
  if (idx > -1 && idx < GetSize() && comparator(key, array[idx].first) == 0)
  {
    value = array[idx].second;
    return true;
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
    const KeyType &key,
    const KeyComparator &comparator)
{
  auto idx = KeyIndex(key, comparator);
  if (idx > -1 && idx < GetSize() && comparator(key, array[idx].first) == 0)
  {
    for (int i = idx; i < GetSize() - 1; i++)
    {
      array[i] = array[i + 1];
    }
    IncreaseSize(-1);
  }

  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page,
 * then update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(
    BPlusTreeLeafPage *recipient,
    int,
    BufferPoolManager *bufferPoolManager,
    const KeyComparator &comparator)
{
  int recipient_sz = recipient->GetSize();
  if (comparator(recipient->array[0].first, array[0].first) < 0)
  {
    for (int i = 0; i < GetSize(); i++)
    {
      recipient->array[recipient_sz + i] = array[i];
    }
    recipient->IncreaseSize(GetSize());
    IncreaseSize(-1 * GetSize());
    recipient->SetNextPageId(GetNextPageId());

    if (GetNextPageId() != INVALID_PAGE_ID)
    {
      Page *page = bufferPoolManager->FetchPage(GetNextPageId());
      auto next_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
      next_page->SetPreviousPageId(recipient->GetPageId());
      bufferPoolManager->UnpinPage(GetNextPageId(), true);
    }
  }
  // recipient->array[0].key > array[0].key
  //
  else
  {
    for (int i = recipient_sz + GetSize() - 1; i >= GetSize(); i--)
    {
      recipient->array[i] = recipient->array[i - GetSize()];
    }
    for (int i = 0; i < GetSize(); i++)
    {
      recipient->array[i] = array[i];
    }
    recipient->IncreaseSize(GetSize());
    IncreaseSize(-1 * GetSize());

    recipient->SetPreviousPageId(GetPreviousPageId());
    if (GetPreviousPageId() != INVALID_PAGE_ID)
    {
      Page *page = bufferPoolManager->FetchPage(GetPreviousPageId());
      auto prev_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
      prev_page->SetNextPageId(recipient->GetPageId());
      bufferPoolManager->UnpinPage(GetPreviousPageId(), true);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(
    MappingType *items,
    int size)
{
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, 
 * then update relevant key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
  assert(GetPageId() == recipient->next_page_id_);
  Page *parent_page = buffer_pool_manager->FetchPage(
      GetParentPageId());

  B_PLUS_TREE_PARENT_PAGE_TYPE *parent = reinterpret_cast<B_PLUS_TREE_PARENT_PAGE_TYPE *>(
      parent_page->GetData());

  MappingType item = GetItem(0);

  parent->SetKeyAt(parent->ValueIndex(GetPageId()), item.first);
  recipient->array[recipient->GetSize()] = item;
  recipient->IncreaseSize(1);

  for (int i = 0; i < GetSize() - 1; i++)
  {
    array[i] = array[i + 1];
  }

  IncreaseSize(-1);
  buffer_pool_manager->UnpinPage(
      GetParentPageId(),
      true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(
    const MappingType &item)
{
}

/*
 * Remove the last key & value pair from this page to "recipient" page,
 * then update relevant key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient,
    int parentIndex,
    BufferPoolManager *buffer_pool_manager)
{
  assert(next_page_id_ == recipient->GetPageId());
  Page *page = buffer_pool_manager->FetchPage(
      GetParentPageId());

  B_PLUS_TREE_PARENT_PAGE_TYPE *parent = reinterpret_cast<B_PLUS_TREE_PARENT_PAGE_TYPE *>(
      page->GetData());

  MappingType item = GetItem(GetSize() - 1);
  IncreaseSize(-1);

  parent->SetKeyAt(
      parent->ValueIndex(recipient->GetPageId()),
      KeyAt(GetSize() - 1));

  for (int i = recipient->GetSize(); i >= 1; i--)
  {
    recipient->array[i] = recipient->array[i - 1];
  }

  recipient->array[0] = item;
  recipient->IncreaseSize(1);

  buffer_pool_manager->UnpinPage(
      GetParentPageId(),
      true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item,
    int parentIndex,
    BufferPoolManager *buffer_pool_manager)
{
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
