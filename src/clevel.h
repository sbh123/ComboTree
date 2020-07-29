#pragma once

#include <libpmemobj++/persistent_ptr.hpp>
#include <cstdint>
#include <cassert>

namespace combotree {

#define CLEVEL_NODE_LEAF_ENTRYS   16
#define CLEVEL_NODE_INDEX_ENTRYS  8

struct Entry {
  uint64_t key;
  union {
    uint64_t value;
    pmem::obj::persistent_ptr<void> pvalue;
  };
};

struct LeafNode {
  pmem::obj::persistent_ptr<struct LeafNode> prev;
  pmem::obj::persistent_ptr<struct LeafNode> next;
  pmem::obj::persistent_ptr<struct IndexNode> parent;
  uint64_t sorted_array;  // used as an array of uint4_t
  struct Entry entry[CLEVEL_NODE_LEAF_ENTRYS];
  uint8_t nr_entry;
  uint8_t next_entry;

  bool Insert(uint64_t key, uint64_t value);
  bool Update(uint64_t key, uint64_t value);
  bool Get(uint64_t key, uint64_t& value);
  bool Delete(uint64_t key);

 private:
  bool Split_();

  /*
   * find entry index which is equal or bigger than key
   *
   * @find   if equal, return true
   * @return entry index
   */
  int Find_(uint64_t key, bool& find) const;

  uint64_t GetSortedArrayMask_(int index) const {
    return (uint64_t)0x0FUL << ((CLEVEL_NODE_LEAF_ENTRYS - 1 - index) * 4);
  }

  /*
   * get entry index in sorted array
   */
  int GetSortedEntry_(int sorted_index) const {
#ifndef NDEBUG
    assert(sorted_index < nr_entry);
#endif // NDEBUG
    uint64_t mask = GetSortedArrayMask_(sorted_index);
    return (sorted_array & mask) >> ((CLEVEL_NODE_LEAF_ENTRYS - 1 - sorted_index) * 4);
  }

  int GetFreeIndex_() const {
#ifndef NDEBUG
    assert(next_entry == CLEVEL_NODE_LEAF_ENTRYS);
#endif // NDEBUG
    int nr_free = next_entry - nr_entry;
    uint64_t mask = (uint64_t)0x0FUL << ((nr_free - 1) * 4);
    return (sorted_array & mask) >> ((nr_free - 1) * 4);
  }

  uint64_t GetEntryKey_(int entry_idx) const {
    return entry[entry_idx].key;
  }

  void PrintSortedArray_() const;
};

struct IndexNode {
  uint64_t keys[CLEVEL_NODE_INDEX_ENTRYS];
  pmem::obj::persistent_ptr<struct LeafNode> child[CLEVEL_NODE_INDEX_ENTRYS + 1];
  uint8_t nr_entry;
  uint8_t sorted_array[CLEVEL_NODE_INDEX_ENTRYS];

  bool Insert(pmem::obj::persistent_ptr<LeafNode> leaf);
  bool Split_();
};

class CLevel {
 public:
  CLevel() {}
  ~CLevel() {}

 private:

};

} // namespace combotree