/**
 * b_plus_tree_page.cpp
 */
#include "page/b_plus_tree_page.h"

namespace cmudb
{

bool BPlusTreePage::IsLeafPage() const
{
    if (page_type_ == IndexPageType::LEAF_PAGE)
    {
        return true;
    }

    return false;
}

bool BPlusTreePage::IsRootPage() const
{
    if (parent_page_id_ == INVALID_PAGE_ID)
    {
        return true;
    }

    return false;
}

void BPlusTreePage::SetPageType(IndexPageType page_type)
{
    page_type_ = page_type;
}

void BPlusTreePage::SetLSN(lsn_t lsn)
{
    lsn_ = lsn;
}

/*
 * Helper methods to get/set size.
 *  Number of key/value pairs stored in that page.
 */
int BPlusTreePage::GetSize() const
{
    return size_;
}

void BPlusTreePage::SetSize(int size)
{
    size_ = size;
}

void BPlusTreePage::IncreaseSize(int amount)
{
    size_ += amount;
}

/*
 * Helper methods to get/set max size of the page
 */
int BPlusTreePage::GetMaxSize() const
{
    return max_size_;
}

void BPlusTreePage::SetMaxSize(int size)
{
    max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * 
 */
int BPlusTreePage::GetMinSize() const
{
    if (IsRootPage())
    {
        if (IsLeafPage())
        {
            return 1;
        }

        return 2;
    }

    return GetMaxSize() / 2;
}

page_id_t BPlusTreePage::GetParentPageId() const
{
    return parent_page_id_;
}

void BPlusTreePage::SetParentPageId(page_id_t parent_page_id)
{
    parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const
{
    return page_id_;
}

void BPlusTreePage::SetPageId(page_id_t page_id)
{
    page_id_ = page_id;
}

} // namespace cmudb
