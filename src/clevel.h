#pragma once

#include <cstdint>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj.h>
#include "combotree/iterator.h"
#include "slab.h"
#include "status.h"
#include "combotree_config.h"
#include "debug.h"

namespace combotree {

#define LEAF_ENTRYS   CLEVEL_LEAF_ENTRY
#define INDEX_ENTRYS  CLEVEL_INDEX_ENTRY

class BLevel;

class CLevel {
 public:
  struct Entry;
  struct LeafNode;
  struct IndexNode;

 public:
  void InitLeaf(pmem::obj::pool_base& pop, Slab<CLevel::LeafNode>* slab);

  Status Insert(pmem::obj::pool_base& pop, Slab<CLevel::LeafNode>* slab, uint64_t key, uint64_t value);
  Status Update(pmem::obj::pool_base& pop, uint64_t key, uint64_t value);
  Status Delete(pmem::obj::pool_base& pop, uint64_t key);
  Status Get(uint64_t key, uint64_t& value) const;
  bool Scan(uint64_t max_key, size_t max_size, size_t& size,
            std::function<void(uint64_t,uint64_t)> callback);

  class Iter;

  Iterator* begin();
  Iterator* end();

  friend BLevel;

 private:
  enum class NodeType {
    LEAF,
    INDEX,
  };

  void* root_;
  LeafNode* head_;
  NodeType type_;

  LeafNode* leaf_root_() const {
    return static_cast<LeafNode*>(root_);
  }

  IndexNode* index_root_() const {
    return static_cast<IndexNode*>(root_);
  }
};

struct CLevel::Entry {
  Entry() : key(0), value(0) {}

  uint64_t key;
  union {
    uint64_t value;
    // pmem::obj::persistent_ptr<void> pvalue;
  };
};

struct CLevel::LeafNode {
  LeafNode() : prev(nullptr), next(nullptr), parent(nullptr),
               sorted_array(0), nr_entry(0), next_entry(0) {}

  LeafNode* prev;
  LeafNode* next;
  IndexNode* parent;
  uint64_t sorted_array;  // used as an array of uint4_t
  uint32_t nr_entry;
  uint32_t next_entry;
  Entry entry[LEAF_ENTRYS];

  Status Insert(pmem::obj::pool_base& pop, Slab<CLevel::LeafNode>* slab, uint64_t key, uint64_t value, void*& root);
  Status Update(pmem::obj::pool_base& pop, uint64_t key, uint64_t value, void*& root);
  Status Get(uint64_t key, uint64_t& value) const;
  Status Delete(pmem::obj::pool_base& pop, uint64_t key);

  void PrintSortedArray() const;

  friend Iter;
  friend CLevel;

 private:
  bool Split_(pmem::obj::pool_base& pop, Slab<CLevel::LeafNode>* slab, void*& root);

  void Valid_();

  /*
   * find entry index which is equal or bigger than key
   *
   * @find   if equal, return true
   * @return entry index
   */
  int Find_(uint64_t key, bool& find) const;

  uint64_t GetSortedArrayMask_(int index) const {
    return (uint64_t)0x0FUL << ((15 - index) * 4);
  }

  /*
   * get entry index in sorted array
   */
  int GetSortedEntry_(int sorted_index) const {
    uint64_t mask = GetSortedArrayMask_(sorted_index);
    return (sorted_array & mask) >> ((15 - sorted_index) * 4);
  }

  int GetFreeIndex_() const {
    assert(next_entry == LEAF_ENTRYS);
    int nr_free = next_entry - nr_entry;
    assert(nr_free > 0);
    uint64_t mask = (uint64_t)0x0FUL << ((nr_free - 1) * 4);
    return (sorted_array & mask) >> ((nr_free - 1) * 4);
  }

  uint64_t GetEntryKey_(int entry_idx) const {
    return entry[entry_idx].key;
  }
};

struct CLevel::IndexNode {
  IndexNode* parent;
  NodeType child_type;
  int nr_entry;
  int next_entry;
  uint64_t keys[INDEX_ENTRYS + 1];
  void* child[INDEX_ENTRYS + 2];
  uint8_t sorted_array[INDEX_ENTRYS + 1];

  Status Insert(pmem::obj::pool_base& pop, Slab<LeafNode>* leaf_slab, uint64_t key, uint64_t value, void*& root);
  Status Update(pmem::obj::pool_base& pop, uint64_t key, uint64_t value, void*& root);
  Status Get(uint64_t key, uint64_t& value) const;
  Status Delete(pmem::obj::pool_base& pop, uint64_t key);

  bool InsertChild(pmem::obj::pool_base& pop, uint64_t child_key, void* child,
                   void*& root);

  friend Iter;

 private:
  bool Split_(pmem::obj::pool_base& pop, void*& root);

  void AdoptChild_(pmem::obj::pool_base& pop);

  LeafNode* FindLeafNode_(uint64_t key) const;

  LeafNode* leaf_child_(int index) const {
    return static_cast<LeafNode*>(child[index]);
  }

  IndexNode* index_child_(int index) const {
    return static_cast<IndexNode*>(child[index]);
  }
};

// TODO: Begin(), End(), CLevel::begin(), CLevel::end()
// is not the same.
class CLevel::Iter : public Iterator {
 public:
  Iter(CLevel* clevel)
      : clevel_(clevel), leaf_(clevel_->head_), sorted_index_(0),
        first_leaf_(clevel_->head_),
        last_leaf_(clevel_->head_->prev),
        is_first_leaf_(true), is_last_leaf_(false)  {}
  ~Iter() {};

  bool Begin() const {
    return is_first_leaf_ && sorted_index_ == 0;
  }

  // leaf_ point to the last, sorted_index_ equals nr_entry
  bool End() const {
    return is_last_leaf_ && sorted_index_ == nr_entry_;
  }

  void SeekToFirst() {
    leaf_ = clevel_->head_;
    while (leaf_ != last_leaf_ &&
           leaf_->nr_entry == 0) {
      leaf_ = leaf_->next;
    }
    UpdateLeaf_();
    sorted_index_ = 0;
  }

  void SeekToLast() {
    leaf_ = clevel_->head_->prev;
    while (leaf_ != first_leaf_ &&
           leaf_->nr_entry == 0) {
      leaf_ = leaf_->prev;
    }
    sorted_index_ = std::max<int>(0, leaf_->nr_entry - 1);
  }

  void Seek(uint64_t target) {
    bool find;
    if (clevel_->type_ == CLevel::NodeType::LEAF) {
      leaf_ = clevel_->leaf_root_();
      sorted_index_ = leaf_->Find_(target, find);
    } else {
      leaf_ = clevel_->index_root_()->FindLeafNode_(target);
      sorted_index_ = leaf_->Find_(target, find);
      if (sorted_index_ == leaf_->nr_entry) {
        if (leaf_ != last_leaf_) {
          do {
            leaf_ = leaf_->next;
          } while (leaf_ != last_leaf_ &&
                  leaf_->nr_entry == 0);
          sorted_index_ = 0;
        }
      }
    }
    UpdateLeaf_();
  }

  void Next() {
    if (sorted_index_ < leaf_->nr_entry - 1) {
      sorted_index_++;
    } else if (is_last_leaf_) {
      sorted_index_ = leaf_->nr_entry;
    } else {
      do {
        leaf_ = leaf_->next;
      } while (leaf_ != last_leaf_ &&
               leaf_->nr_entry == 0);
      UpdateLeaf_();
      sorted_index_ = 0;
    }
  }

  void Prev() {
    if (sorted_index_ > 0) {
      sorted_index_--;
    } else if (is_first_leaf_) {
      sorted_index_ = 0;
    } else {
      do {
        leaf_ = leaf_->prev;
      } while (leaf_ != first_leaf_ &&
               leaf_->nr_entry == 0);
      UpdateLeaf_();
      sorted_index_ = std::max<int>(0, leaf_->nr_entry - 1);
    }
  }

  uint64_t key() const {
    int entry_idx = GetSortedEntry_();
    return entry_[entry_idx].key;
  }

  uint64_t value() const {
    int entry_idx = GetSortedEntry_();
    return entry_[entry_idx].value;
  }

 private:
  CLevel* clevel_;
  LeafNode* leaf_;
  uint32_t sorted_index_;
  // cache these information in RAM
  LeafNode* first_leaf_;
  LeafNode* last_leaf_;
  uint64_t sorted_array_;
  CLevel::Entry* entry_;
  bool is_first_leaf_;
  bool is_last_leaf_;
  int nr_entry_;

  void UpdateLeaf_() {
    is_first_leaf_ = leaf_ == first_leaf_;
    is_last_leaf_ = leaf_ == last_leaf_;
    nr_entry_ = leaf_->nr_entry;
    sorted_array_ = leaf_->sorted_array;
    entry_ = leaf_->entry;
  }

  uint64_t GetSortedArrayMask_(int index) const {
    return (uint64_t)0x0FUL << ((15 - index) * 4);
  }

  /*
   * get entry index in sorted array
   */
  int GetSortedEntry_() const {
    uint64_t mask = (uint64_t)0x0FUL << ((15 - sorted_index_) * 4);
    return (sorted_array_ & mask) >> ((15 - sorted_index_) * 4);
  }

};

} // namespace combotree