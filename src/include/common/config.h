/**
 * config.h
 *
 * Database system configuration
 */

#include <cstdint>

namespace cmudb
{

#define INVALID_PAGE_ID -1 // representing an invalid page id
#define INVALID_TXN_ID -1  // representing an invalid txn id
#define INVALID_LSN -1     // representing an invalid lsn
#define HEADER_PAGE_ID 0   // the header page id
#define PAGE_SIZE 512      // size of a data page in byte
#define BUCKET_SIZE 50     // size of extendible hash bucket

// #define BUFFER_POOL_SIZE 10 // size of buffer pool

typedef int32_t lsn_t;     // log seq type
typedef int32_t page_id_t; // page id type
typedef int32_t txn_id_t;  // transaction id type

} // namespace cmudb
