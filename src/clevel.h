#pragma once

#include <cstdint>
#include <cassert>
#include <libpmemobj++/persistent_ptr.hpp>
#include "iterator.h"

namespace combotree {

#define CLEVEL_NODE_LEAF_ENTRYS   16
#define CLEVEL_NODE_INDEX_ENTRYS  8

struct Entry {
  Entry() : key(0), value(0) {}

  uint64_t key;
  union {
    uint64_t value;
    pmem::obj::persistent_ptr<void> pvalue;
  };
};

class CLevel;

struct LeafNode {
  LeafNode() : prev(nullptr), next(nullptr), parent(nullptr),
               sorted_array(0), nr_entry(0), next_entry(0) {}

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

  void PrintSortedArray() const;

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
    // assert(sorted_index < nr_entry);
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

  friend class CLevel;

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
  class Iter;

  bool Insert(uint64_t key, uint64_t value);
  bool Update(uint64_t key, uint64_t value);
  bool Get(uint64_t key, uint64_t& value);
  bool Delete(uint64_t key);

  Iterator* begin();
  Iterator* end();

 private:
  LeafNode leaf_;
};

class CLevel::Iter : public Iterator {
 public:
  Iter(CLevel* clevel) : sorted_index_(0), clevel_(clevel) {}
  ~Iter() {};

  bool Begin() const { return sorted_index_ == 0; }

  bool End() const { return sorted_index_ == clevel_->leaf_.nr_entry - 1; }

  void SeekToFirst() { sorted_index_ = 0; }

  void SeekToLast() { sorted_index_ = clevel_->leaf_.nr_entry - 1; }

  void Seek(uint64_t target) {
    bool find;
    int idx = clevel_->leaf_.Find_(target, find);
    if (find) sorted_index_ = idx;
  }

  void Next() { sorted_index_++; }

  void Prev() { sorted_index_--; }

  uint64_t key() const {
    int entry_idx = clevel_->leaf_.GetSortedEntry_(sorted_index_);
    return clevel_->leaf_.entry[entry_idx].key;
  }

  uint64_t value() const {
    int entry_idx = clevel_->leaf_.GetSortedEntry_(sorted_index_);
    return clevel_->leaf_.entry[entry_idx].value;
  }

 private:
  int sorted_index_;
  CLevel* clevel_;
};

} // namespace combotree