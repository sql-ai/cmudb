/**
 * b_plus_tree_page.h
 *
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 24 bytes in total):
 * ----------------------------------------------------------------------------
 * |PageType(4)|CurrentSize(4)|MaxSize(4)|ParentPageId(4)|PageId(4)|LogSeqNo(4)|
 * ----------------------------------------------------------------------------
 * 
 * Type         4   Page Type (internal or leaf)
 * Size         4   Number of Key & Value pairs in page
 * max_size     4   Max number of Key & Value pairs in page
 * parent_id    4   Parent Page Id
 * page_id      4   Self Page Id
 * lsn          4   Log sequence Number
 */

#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "index/generic_key.h"

namespace cmudb
{

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS \
  template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
enum class IndexPageType
{
  INVALID_INDEX_PAGE = 0,
  LEAF_PAGE,
  INTERNAL_PAGE
};

// Abstract class.
// This is the parent class that both the Internal Page and Leaf Page
// inherited from. Contains information that both child classes share.
class BPlusTreePage
{
public:
  bool IsLeafPage() const;
  bool IsRootPage() const;
  void SetPageType(IndexPageType page_type);

  int GetSize() const;
  void SetSize(int size);
  void IncreaseSize(int amount);

  int GetMaxSize() const;
  void SetMaxSize(int max_size);
  int GetMinSize() const;

  void SetLSN(lsn_t lsn = INVALID_LSN);

  page_id_t GetParentPageId() const;
  void SetParentPageId(page_id_t parent_page_id);

  page_id_t GetPageId() const;
  void SetPageId(page_id_t page_id);

private:
  // Member variables.
  // Attributes that both internal and leaf page share.
  //
  IndexPageType page_type_;
  int size_;
  int max_size_;
  page_id_t parent_page_id_;
  page_id_t page_id_;
  lsn_t lsn_;
};

} // namespace cmudb
