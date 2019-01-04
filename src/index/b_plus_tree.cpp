/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb
{

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                          BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator,
                          page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const
{
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction)
{
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key);
  Page *page = reinterpret_cast<Page *>(leaf_page);
  ValueType value;
  if (leaf_page->Lookup(key, value, comparator_))
  {
    result.push_back(value);
    return true;
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction)
{
  if (IsEmpty())
  {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value)
{
  Page *page = buffer_pool_manager_->NewPage(root_page_id_);
  if (page == nullptr)
  {
    throw std::bad_alloc();
  }
  UpdateRootPageId();
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);
  leaf_page->Init(root_page_id_);
  leaf_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction)
{
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key);
  Page *page = reinterpret_cast<Page *>(leaf_page);
  ValueType value_copy = value;
  if (!leaf_page->Lookup(key, value_copy, comparator_))
  {
    if (leaf_page->Insert(key, value, comparator_) > leaf_page->GetMaxSize())
    {
      InsertIntoParent(leaf_page, leaf_page->KeyAt(leaf_page->GetMinSize() - 1), Split(leaf_page));
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return true;
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return false;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node)
{
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr)
  {
    throw std::bad_alloc();
  }
  N *new_node = reinterpret_cast<N *>(page);
  new_node->Init(page_id, node->GetParentPageId());
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction)
{
  if (old_node->IsRootPage())
  {
    page_id_t parent_page_id;
    Page *page = buffer_pool_manager_->NewPage(parent_page_id);
    root_page_id_ = parent_page_id;
    UpdateRootPageId();
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
    parent_page->Init(parent_page_id);
    parent_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(parent_page_id, true);

    page = reinterpret_cast<Page *>(old_node);
    old_node->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

    page = reinterpret_cast<Page *>(new_node);
    new_node->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
  else
  {
    page_id_t parent_page_id = old_node->GetParentPageId();
    Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_page =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

    if (parent_page->GetSize() > parent_page->GetMaxSize())
    {
      InsertIntoParent(parent_page, parent_page->KeyAt(parent_page->GetMinSize()), Split(parent_page));
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

    page = reinterpret_cast<Page *>(old_node);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

    page = reinterpret_cast<Page *>(new_node);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction)
{
  if (IsEmpty())
  {
    return;
  }
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key);
  int size = leaf_page->RemoveAndDeleteRecord(key, comparator_);
  if (!leaf_page->IsRootPage() && size < leaf_page->GetMinSize())
  {
    if (CoalesceOrRedistribute(leaf_page, transaction))
    {
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
      assert(buffer_pool_manager_->DeletePage(leaf_page->GetPageId()));
      return;
    }
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node,
                                            Transaction *transaction)
{
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_node =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page);

  int node_index = parent_node->ValueIndex(node->GetPageId());
  int sibling_index = node_index + 1;
  if (sibling_index >= parent_node->GetSize())
  {
    sibling_index = node_index - 1;
  }
  int sibling_id = parent_node->ValueAt(sibling_index);
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
  N *sibling = reinterpret_cast<N *>(sibling_page);
  if (sibling->GetSize() + node->GetSize() > node->GetMaxSize())
  {
    if (sibling_index > node_index)
      Redistribute(sibling, node, 0);
    else
      Redistribute(sibling, node, 1);
    return false;
  }
  else
  {
    if (sibling_index < node_index)
      Coalesce(sibling, node, parent_node, 0, transaction);
    else
      Coalesce(sibling, node, parent_node, 1, transaction);
    return true;
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node,
    N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index,
    Transaction *transaction)
{
  assert(index == 0 || index == 1);
  if (index == 0)
  {
    node->MoveAllTo(neighbor_node, parent->ValueIndex(node->GetPageId()), buffer_pool_manager_);
  }
  else
  {
    node->MoveAllTo(neighbor_node, parent->ValueIndex(neighbor_node->GetPageId()), buffer_pool_manager_);
    parent->SetValueAt(parent->ValueIndex(node->GetPageId()), neighbor_node->GetPageId());
  }

  Page *page = reinterpret_cast<Page *>(node);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  page = reinterpret_cast<Page *>(neighbor_node);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  buffer_pool_manager_->DeletePage(node->GetPageId());

  if (parent->IsRootPage())
  {
    return AdjustRoot(parent);
  }
  else
  {
    return CoalesceOrRedistribute(parent);
  }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index)
{
  if (index == 0)
  {
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  }
  else
  {
    neighbor_node->MoveLastToFrontOf(node, 0, buffer_pool_manager_);
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node)
{
  int size = old_root_node->GetSize();
  if (size == 1)
  {
    if (old_root_node->IsLeafPage())
    {
      return false;
    }

    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_page =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);

    root_page_id_ = parent_page->ValueAt(0);
    Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage *new_root_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  }
  if (size == 0 || size == 1)
  {
    Page *page = reinterpret_cast<Page *>(old_root_node);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    UpdateRootPageId();
    root_page_id_ = INVALID_PAGE_ID;
    return true;
  }

  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin()
{
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(KeyType(), true);
  return INDEXITERATOR_TYPE(*buffer_pool_manager_, leaf_page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key)
{
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key);
  return INDEXITERATOR_TYPE(*buffer_pool_manager_, leaf_page, leaf_page->KeyIndex(key, comparator_));
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost)
{
  if (IsEmpty())
  {
    return nullptr;
  }
  page_id_t page_id = root_page_id_;
  Page *page = (buffer_pool_manager_->FetchPage(page_id));
  BPlusTreePage *bplustree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!bplustree_page->IsLeafPage() && bplustree_page != nullptr)
  {
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *internal_page =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(bplustree_page);

    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = leftMost ? internal_page->ValueAt(0) : internal_page->Lookup(key, comparator_);
    page = (buffer_pool_manager_->FetchPage(page_id));
    bplustree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(bplustree_page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record)
{
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input)
  {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input)
  {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
