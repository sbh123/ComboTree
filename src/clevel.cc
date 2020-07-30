#include <iostream>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include "clevel.h"

namespace combotree {

bool CLevel::Insert(uint64_t key, uint64_t value) {
  return leaf_.Insert(key, value);
}

bool CLevel::Update(uint64_t key, uint64_t value) {
  return leaf_.Update(key, value);
}

bool CLevel::Get(uint64_t key, uint64_t& value) {
  return leaf_.Get(key, value);
}

bool CLevel::Delete(uint64_t key) {
  return leaf_.Delete(key);
}

// find entry index which is less or equal to key
int LeafNode::Find_(uint64_t key, bool& find) const {
  int left = 0;
  int right = nr_entry - 1;
  // binary search
  while (left <= right) {
    int middle = (left + right) / 2;
    int mid_idx = GetSortedEntry_(middle);
    uint64_t mid_key = GetEntryKey_(mid_idx);
    if (mid_key == key) {
      find = true;
      return middle; // find
    } else if (mid_key < key) {
      left = middle + 1;
    } else {
      right = middle - 1;
    }
  }
  // not found, return first entry bigger than key
  find = false;
  return left;
}

void LeafNode::PrintSortedArray() const {
  for (int i = 0; i < 16; ++i) {
    std::cout << i << ": " << GetSortedEntry_(i) << std::endl;
  }
}

bool LeafNode::Split_() {
#ifndef NDEBUG
  assert(nr_entry == CLEVEL_NODE_LEAF_ENTRYS);
#endif // NDEBUG

  if (parent == nullptr) {
    parent = pmem::obj::make_persistent<IndexNode>();
    parent->child[0] = this;
  }

  pmem::obj::persistent_ptr<struct LeafNode> new_node =
      pmem::obj::make_persistent<struct LeafNode>();
  uint64_t new_sorted_array = 0;
  for (int i = 0; i < CLEVEL_NODE_LEAF_ENTRYS / 2; ++i) {
    new_node->entry[i].key = entry[i + CLEVEL_NODE_LEAF_ENTRYS / 2].key;
    new_node->entry[i].pvalue = entry[i + CLEVEL_NODE_LEAF_ENTRYS / 2].pvalue;
    new_sorted_array = (new_sorted_array << 4) | i;
  }
  new_node->prev = this;
  new_node->next = next;
  new_node->parent = parent;
  new_node->nr_entry = CLEVEL_NODE_LEAF_ENTRYS / 2;
  new_node->next_entry = CLEVEL_NODE_LEAF_ENTRYS / 2;
  new_node->sorted_array = new_sorted_array <<
      ((CLEVEL_NODE_LEAF_ENTRYS - CLEVEL_NODE_LEAF_ENTRYS / 2) * 4);
  new_node.persist();

  nr_entry = CLEVEL_NODE_LEAF_ENTRYS - CLEVEL_NODE_LEAF_ENTRYS / 2;
  next = new_node;
  parent->Insert(new_node);
  return true;
}

bool LeafNode::Insert(uint64_t key, uint64_t value) {
  bool find;
  int sorted_index = Find_(key, find);
  // already exist
  if (find) return false;

  // if not find, insert key in index
  if (nr_entry == CLEVEL_NODE_LEAF_ENTRYS) {
    // full, split
    Split_();
    // key should insert to the newly split node
    if (key > next->entry[0].key)
      return next->Insert(key, value);
  }

  uint64_t new_sorted_array;
  int entry_idx = (next_entry != CLEVEL_NODE_LEAF_ENTRYS) ? next_entry
                                                          : GetFreeIndex_();

  uint64_t free_mask =
      next_entry == (nr_entry + 1) ? 0 : (~0x00UL) >> ((nr_entry + 1) * 4);
  uint64_t free_index = sorted_array & free_mask;

  uint64_t after_mask = 0x0FUL;
  for (int i = 0; i < nr_entry - sorted_index - 1; ++i)
    after_mask = (after_mask << 4) | 0x0FUL;
  after_mask = sorted_index == nr_entry ? 0 :
      after_mask << ((CLEVEL_NODE_LEAF_ENTRYS - nr_entry) * 4);
  uint64_t after_index = (sorted_array & after_mask) >> 4;

  uint64_t before_mask =
      sorted_index == 0 ? 0 : ~((~0x00UL) >> (sorted_index * 4));
  uint64_t before_index = sorted_array & before_mask;
  new_sorted_array =
      before_index |
      ((uint64_t)entry_idx << ((CLEVEL_NODE_LEAF_ENTRYS - 1 - sorted_index) * 4)) |
      after_index |
      free_index;

  entry[entry_idx].key = key;
  entry[entry_idx].value = value;
  nr_entry++;
  if (entry_idx == next_entry) next_entry++;
  sorted_array = new_sorted_array;

  // PrintSortedArray();
  return true;
}

bool LeafNode::Delete(uint64_t key) {
  bool find;
  int sorted_index = Find_(key, find);
  if (!find) {
    // key not exist
    return false;
  }

  int entry_index = GetSortedEntry_(sorted_index);

  uint64_t free_mask = next_entry == nr_entry ? 0 : ~((~0x00UL) << ((next_entry - nr_entry) * 4));
  uint64_t free_index = sorted_array & free_mask;

  uint64_t after_mask = 0x0FUL;
  for (int i = 0; i < nr_entry - sorted_index - 2; ++i)
    after_mask = (after_mask << 4) | 0x0FUL;
  after_mask = sorted_index == (nr_entry - 1) ? 0 :
      after_mask << ((CLEVEL_NODE_LEAF_ENTRYS - nr_entry) * 4);
  uint64_t after_index = (sorted_array & after_mask) << 4;

  uint64_t before_mask = sorted_index == 0 ? 0 : ~((~0x00UL) >> (sorted_index * 4));
  uint64_t before_index = sorted_array & before_mask;
  uint64_t new_sorted_array =
      before_index |
      after_index |
      ((uint64_t)entry_index << ((next_entry - nr_entry) * 4)) |
      free_index;

  sorted_array = new_sorted_array;
  nr_entry--;

  // PrintSortedArray();
  return true;
}

bool LeafNode::Update(uint64_t key, uint64_t value) {
  bool find;
  int sorted_index = Find_(key, find);
  if (!find) {
    // key not exist
    return false;
  }

  int entry_index = GetSortedEntry_(sorted_index);
  entry[entry_index].value = value;
  return true;
}

bool LeafNode::Get(uint64_t key, uint64_t& value) {
  bool find;
  int sorted_index = Find_(key, find);
  if (!find) {
    // key not exist
    return false;
  }

  int entry_index = GetSortedEntry_(sorted_index);
  value = entry[entry_index].value;
  return true;
}

bool IndexNode::Split_() {
  return true;
}

bool IndexNode::Insert(pmem::obj::persistent_ptr<LeafNode> leaf) {
  assert(nr_entry < CLEVEL_NODE_INDEX_ENTRYS);
  uint64_t leaf_key = leaf->entry[0].key;
  int left = 0;
  int right = nr_entry - 1;

  while (left <= right) {
    int middle = (left + right) / 2;
    int idx = sorted_array[middle];
    if (keys[idx] == leaf_key) {
      assert(0);
    } else if (keys[idx] < leaf_key) {
      left = middle + 1;
    } else {
      right = middle - 1;
    }
  }

  uint8_t new_sorted_array[CLEVEL_NODE_INDEX_ENTRYS];
  memcpy(new_sorted_array, sorted_array, sizeof(sorted_array));
  for (int i = nr_entry; i > left; --i)
    new_sorted_array[i] = new_sorted_array[i - 1];
  new_sorted_array[left] = nr_entry;

  keys[nr_entry] = leaf->entry[0].key;
  child[nr_entry + 1] = leaf;
  memcpy(sorted_array, new_sorted_array, sizeof(sorted_array));
  nr_entry++;

  return true;
}

Iterator* CLevel::begin() {
  Iterator* iter = new Iter(this);
  iter->SeekToFirst();
  return iter;
}

Iterator* CLevel::end() {
  Iterator* iter = new Iter(this);
  iter->SeekToLast();
  return iter;
}

} // namespace combotree