/**
 * b_plus_tree_internal_page.cpp
 * 
 * #define B_PLUS_TREE_INTERNAL_PAGE_TYPE \
 *  BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
 * 
 * #define INDEX_TEMPLATE_ARGUMENTS \
 * template <typename KeyType, typename ValueType, typename KeyComparator>
 * 
 */
#include "page/b_plus_tree_internal_page.h"
#include "common/exception.h"
#include <iostream>
#include <sstream>

namespace cmudb
{
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(
    page_id_t page_id,
    page_id_t parent_id)
{
  SetPageId(page_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetParentPageId(parent_id);
  size_t internal_page_size = sizeof(BPlusTreeInternalPage);
  size_t max_size = (PAGE_SIZE - internal_page_size) / sizeof(MappingType) - 1;
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(
    int index) const
{
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(
    int index,
    const KeyType &key)
{
  MappingType kv = array[index];
  kv.first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(
    const ValueType &value) const
{
  for (int i = 0; i < GetSize(); i++)
  {
    // ValueType is page_id.
    // Dont define ValueComparator until needed
    //
    if (array[i].second == value)
    {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const
{
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(
    const KeyType &key,
    const KeyComparator &comparator) const
{
  int begin = 1;
  int end = GetSize();
  while (begin < end)
  {
    int mid = begin + (end - begin) / 2;
    int cmp = comparator(key, array[mid].first);
    if (cmp > 0)
    {
      begin = mid + 1;
    }
    else
    {
      end = mid;
    }
  }
  return array[begin - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value,
    const KeyType &new_key,
    const ValueType &new_value)
{
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  IncreaseSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value,
    const KeyType &new_key,
    const ValueType &new_value)
{
  assert(GetSize() <= GetMaxSize());
  int old_idx = ValueIndex(old_value);
  assert(old_idx > -1);
  int old_size = GetSize();
  for (int i = old_size; i > old_idx + 1; i--)
  {
    array[i] = array[i - 1];
  }
  array[old_idx + 1].first = new_key;
  array[old_idx + 1].second = new_value;
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
  int half_size = GetMaxSize() / 2;

  // recipient points to an empty BPlusTreeInternalPage
  //
  assert(recipient->GetSize() == 0);

  int new_recipient_size = GetSize() - half_size;
  for (int i = 0; i < new_recipient_size; i++)
  {
    recipient->array[i] = array[half_size + i];
  }

  SetSize(half_size);
  recipient->SetSize(new_recipient_size);

  for (int j = 0; j < new_recipient_size; j++)
  {
    page_id_t page_id = static_cast<page_id_t>(recipient->ValueAt(j));
    auto *page = buffer_pool_manager->FetchPage(page_id);
    BPlusTreePage *page_ptr = reinterpret_cast<BPlusTreePage *>(page);
    page_ptr->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(page_ptr->GetPageId(), true);
  }
}

// Empty for now
//
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items,
    int size,
    BufferPoolManager *buffer_pool_manager)
{
  return;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index)
{
  assert(index < GetSize());
  for (int i = index + 1; i < GetSize(); i++)
  {
    array[i - 1] = array[i];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild()
{
  return INVALID_PAGE_ID;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient,
    int index_in_parent,
    BufferPoolManager *buffer_pool_manager,
    const KeyComparator &comparator)
{
  assert(this->GetParentPageId() == recipient->GetParentPageId());

  auto *parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent_ptr = reinterpret_cast<BPlusTreeInternalPage *>(parent_page);
  KeyType parent_key = parent_ptr->KeyAt(index_in_parent);

  int my_size = GetSize();
  int recipient_size = recipient->GetSize();

  // KeyAt(0) is not used.
  //
  KeyType my_key = KeyAt(1);
  KeyType recipient_key = recipient->KeyAt(1);
  if (comparator(my_key, recipient_key) < 0)
  {
    for (int i = my_size + recipient_size - 1; i >= my_size; i--)
    {
      recipient->array[i] = recipient->array[i - my_size];
    }
    recipient->array[my_size].first = parent_key;
    for (int j = 0; j < my_size; j++)
    {
      recipient->array[j] = array[j];
    }
  }
  else
  {
    for (int i = 0; i < my_size; i++)
    {
      recipient->array[recipient_size + i] = array[i];
    }
    recipient->array[recipient_size].first = parent_key;
  }
  recipient->IncreaseSize(my_size);
  for (int i = 0; i < my_size; i++)
  {
    page_id_t page_id = array[i].second;
    auto *page = buffer_pool_manager->FetchPage(page_id);
    BPlusTreePage *page_ptr = reinterpret_cast<BPlusTreePage *>(page);
    page_ptr->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
  }
  buffer_pool_manager->UnpinPage(GetParentPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items,
    int size,
    BufferPoolManager *buffer_pool_manager)
{
  return;
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relevant key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{

  int my_size = GetSize();
  int recipient_size = recipient->GetSize();

  // Copy first key & value pair to tail of recipient
  //
  recipient->array[recipient_size] = array[0];
  recipient->IncreaseSize(1);
  for (int i = 0; i < my_size - 1; i++)
  {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);

  // Find parent idx
  //
  auto *parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent_ptr = reinterpret_cast<BPlusTreeInternalPage *>(parent_page);
  int parent_idx = parent_ptr->ValueIndex(GetPageId());

  recipient->SetKeyAt(recipient->GetSize() - 1, parent_ptr->KeyAt(parent_idx));
  parent_ptr->SetKeyAt(parent_idx, KeyAt(0));
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);

  //moved page
  page_id_t moved_page_id = recipient->ValueAt(recipient->GetSize() - 1);
  auto *moved_page = buffer_pool_manager->FetchPage(moved_page_id);
  BPlusTreeInternalPage *moved_page_ptr = reinterpret_cast<BPlusTreeInternalPage *>(moved_page);
  moved_page_ptr->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(moved_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair,
    BufferPoolManager *buffer_pool_manager)
{
  return;
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient,
    int parent_index,
    BufferPoolManager *buffer_pool_manager)
{
  int my_size = GetSize();
  int recipient_size = recipient->GetSize();
  for (int i = recipient_size; i > 0; i--)
  {
    recipient->array[i] = recipient->array[i - 1];
  }
  recipient->array[0] = array[my_size - 1];

  recipient->IncreaseSize(1);
  IncreaseSize(-1);

  auto *parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent_ptr = reinterpret_cast<BPlusTreeInternalPage *>(parent_page);
  int parent_idx = parent_ptr->ValueIndex(recipient->GetPageId());
  recipient->SetKeyAt(parent_idx, recipient->KeyAt(0));
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);

  //moved page
  page_id_t moved_page_id = recipient->ValueAt(0);
  auto *moved_page = buffer_pool_manager->FetchPage(moved_page_id);
  BPlusTreeInternalPage *moved_page_ptr = reinterpret_cast<BPlusTreeInternalPage *>(moved_page);
  moved_page_ptr->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(moved_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair,
    int parent_index,
    BufferPoolManager *buffer_pool_manager)
{
  return;
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager)
{
  for (int i = 0; i < GetSize(); i++)
  {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const
{
  if (GetSize() == 0)
  {
    return "";
  }
  std::ostringstream os;
  if (verbose)
  {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
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
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose)
    {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(
    int index,
    const ValueType &v)
{
  assert(index <= GetSize());
  array[index].second = v;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                     GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                     GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                     GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                     GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                     GenericComparator<64>>;
} // namespace cmudb
