/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

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
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetSize(0);
  SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / sizeof(MappingType) - 1);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const
{
  KeyType key = array[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key)
{
  assert(index >= 0 && index < GetMaxSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const
{
  for (int i = 0, n = GetSize(); i < n; i++)
  {
    if (array[i].second == value)
      return i;
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

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(
    int index,
    const ValueType &v)
{
  assert(index <= GetSize());
  array[index].second = v;
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
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const
{
  int start = 1;
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
  return array[start - 1].second;
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
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value)
{
  IncreaseSize(2);
  array[0].second = old_value;
  array[1].second = new_value;
  array[1].first = new_key;
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value)
{
  int old_value_index = ValueIndex(old_value);
  int end = GetSize();
  for (int i = old_value_index + 1; i < end; i++)
  {
    array[i + 1] = array[i];
  }
  array[old_value_index + 1].first = new_key;
  array[old_value_index + 1].second = new_value;
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
  assert(recipient->GetSize() == 0);
  recipient->CopyHalfFrom(array, GetSize(), buffer_pool_manager);
  SetSize(GetMinSize());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager)
{
  assert(GetSize() == 0);
  SetSize(size);
  for (int i = 0; i < size - GetMinSize(); i++)
  {
    array[0] = items[0];
    page_id_t page_id = array[i].second;
    Page *page = buffer_pool_manager->FetchPage(page_id);
    BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page);
    tree_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(tree_page->GetPageId(), true);
  }
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
  SetSize(0);
  return array[0].second;
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
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager)
{
  recipient->CopyAllFrom(array, GetSize(), buffer_pool_manager);
  SetSize(0);

  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(page);
  parent_page->Remove(index_in_parent);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager)
{
  int old_size = GetSize();
  IncreaseSize(size);
  for (int i = 0; i < size; i++)
  {
    array[old_size + i] = items[i];
    page_id_t page_id = items[i].second;
    Page *page = buffer_pool_manager->FetchPage(page_id);
    BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page);
    tree_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(tree_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
  recipient->CopyLastFrom(array[0], buffer_pool_manager);
  Remove(0);
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(page);
  int index_in_parent = parent_page->ValueIndex(GetPageId());
  parent_page->SetKeyAt(index_in_parent, array[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager)
{
  array[GetSize()] = pair;
  IncreaseSize(1);
  Page *page = buffer_pool_manager->FetchPage(pair.second);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page);
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager)
{
  recipient->CopyFirstFrom(array[GetSize() - 1], parent_index, buffer_pool_manager);
  Remove(GetSize() - 1);

  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(page);

  int index_in_parent = parent_page->ValueIndex(GetPageId());
  parent_page->SetKeyAt(index_in_parent, array[GetSize() - 1].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager)
{
  for (int i = 0, size = GetSize(); i < size; i++)
  {
    array[i + 1] = array[i];
  }
  IncreaseSize(1);
  array[0] = pair;
  Page *page = buffer_pool_manager->FetchPage(pair.second);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page);
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(GetPageId(), true);
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