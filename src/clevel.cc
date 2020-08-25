#include <iostream>
#include <set>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj.h>
#include "slab.h"
#include "clevel.h"
#include "debug.h"

namespace combotree {

using pmem::obj::persistent_ptr;
using pmem::obj::persistent_ptr_base;
using pmem::obj::make_persistent_atomic;
using pmem::obj::pool_base;

void CLevel::LeafNode::Valid_() {
  std::set<int> idx;
  for (uint32_t i = 0; i < nr_entry; ++i) {
    int index = GetSortedEntry_(i);
    assert(idx.count(index) == 0);
    idx.emplace(index);
  }
  for (uint32_t i = 0; i < LEAF_ENTRYS - nr_entry; ++i) {
    int index = GetSortedEntry_(15 - i);
    assert(idx.count(index) == 0);
    idx.emplace(index);
  }
}

void CLevel::InitLeaf(MemoryManagement* mem) {
  head_ = mem->NewLeafNode();
  head_->id = mutex_.AllocateId();
  root_ = head_;
  mem->persist(head_, sizeof(*head_));
  mem->persist(this, sizeof(*this));
  assert(head_);
}

Status CLevel::Insert(MemoryManagement* mem, uint64_t key, uint64_t value) {
  Node root = root_;
  if (root.IsLeaf())
    return root.leaf()->Insert(mem, mutex_, key, value, &root_);
  else
    return root.index()->Insert(mem, mutex_, key, value, &root_);
}

Status CLevel::Update(MemoryManagement* mem, uint64_t key, uint64_t value) {
  Node root = root_;
  if (root.IsLeaf())
    return root.leaf()->Update(mem, mutex_, key, value, &root_);
  else
    return root.index()->Update(mem, mutex_, key, value, &root_);
}

Status CLevel::Get(MemoryManagement* mem, uint64_t key, uint64_t& value) const {
  Node root = root_;
  if (root.IsLeaf())
    return root.leaf()->Get(mem, key, value);
  else
    return root.index()->Get(mem, key, value);
}

Status CLevel::Delete(MemoryManagement* mem, uint64_t key) {
  Node root = root_;
  if (root.IsLeaf())
    return root.leaf()->Delete(mem, mutex_, key);
  else
    return root.index()->Delete(mem, mutex_, key);
}

bool CLevel::Scan(MemoryManagement* mem, uint64_t max_key, size_t max_size,
                  size_t& size, std::function<void(uint64_t,uint64_t)> callback) {
  return head_->Scan_(mem, max_key, max_size, size, 0, callback);
}

bool CLevel::Scan(MemoryManagement* mem, uint64_t min_key, uint64_t max_key,
                  size_t max_size, size_t& size, std::function<void(uint64_t,uint64_t)> callback) {
  Node root = root_;
  if (root.IsLeaf())
    return root.leaf()->Scan(mem, min_key, max_key, max_size, size, callback);
  else
    return root.index()->Scan(mem, min_key, max_key, max_size, size, callback);
}

// find sorted index which is bigger or equal to key
int CLevel::LeafNode::Find_(uint64_t key, bool& find) const {
  int left = 0;
  int right = nr_entry - 1;
  // binary search
  while (left <= right) {
    int middle = (left + right) / 2;
    uint64_t mid_key = GetSortedKey_(middle);
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

void CLevel::LeafNode::PrintSortedArray() const {
  for (int i = 0; i < 16; ++i) {
    std::cout << i << ": " << GetSortedEntry_(i) << std::endl;
  }
}

CLevel::LeafNode* CLevel::LeafNode::Split_(MemoryManagement* mem, Mutex& mutex,
                                           Node* root) {
  // LOG(Debug::INFO, "Split");
  assert(nr_entry == LEAF_ENTRYS);

  // lock global mutex
  std::lock_guard<std::mutex> lock(mutex.GetGlobalMutex());

  // get parent
  IndexNode* parent;
  if (root->IsLeaf()) {
    // allocate new root
    parent = mem->NewIndexNode();
    parent->child[0] = this;
    mem->persist(parent, sizeof(*parent));
  } else {
    parent = root->index()->FindParent(GetSortedKey_(0), this);
  }

  // copy bigger half to new node
  LeafNode* new_node = mem->NewLeafNode();
  for (int i = 0; i < LEAF_ENTRYS / 2; ++i) {
    new_node->entry[i].key = entry[GetSortedEntry_(i + LEAF_ENTRYS - LEAF_ENTRYS / 2)].key;
    new_node->entry[i].value = entry[GetSortedEntry_(i + LEAF_ENTRYS - LEAF_ENTRYS / 2)].value;
  }
  new_node->id = mutex.AllocateId();
  new_node->next = next;
  new_node->min_key = new_node->GetSortedKey_(0);
  new_node->nr_entry = LEAF_ENTRYS / 2;
  mem->persist(new_node, sizeof(*new_node));

  // insert new node to parent
  parent->InsertChild(mem, new_node->GetSortedKey_(0), new_node, root);

  // change root
  if (root->IsLeaf()) {
    Node new_root(parent);
    *root = new_root;
    mem->persist(root, sizeof(*root));
  }

  // change next ptr
  SetNext(mem->BaseAddr(), new_node);
  mem->persist(&next, sizeof(next));

  // change metadata
  nr_entry = LEAF_ENTRYS - LEAF_ENTRYS / 2;
  mem->persist(&meta_data, sizeof(meta_data));

  return new_node;
}

Status CLevel::LeafNode::Insert(MemoryManagement* mem, Mutex& mutex, uint64_t key,
                                uint64_t value, Node* root) {
  std::lock_guard<std::mutex> lock(mutex.GetLeafMutex(id));

  // if not find, insert key in index
  if (nr_entry == LEAF_ENTRYS) {
    // full, split
    LeafNode* new_node = Split_(mem, mutex, root);
    // key should insert to the newly split node
    if (key >= new_node->min_key)
      return new_node->Insert(mem, mutex, key, value, root);
  }

  bool find;
  int sorted_index = Find_(key, find);
  // already exist
  if (find) return Status::ALREADY_EXISTS;

  // special situation, this node has split
  if (sorted_index == nr_entry) {
    LeafNode* next = GetNext(mem->BaseAddr());
    if (next && key >= next->min_key)
      return next->Insert(mem, mutex, key, value, root);
  }

  int free_index = GetFreeEntry_();
  PutEntry_(mem, free_index, key, value);
  PutSortedArray_(mem, sorted_index, free_index);

  // Valid_();
  // PrintSortedArray();
  return Status::OK;
}

Status CLevel::LeafNode::Delete(MemoryManagement* mem, Mutex& mutex, uint64_t key) {
  std::lock_guard<std::mutex> lock(mutex.GetLeafMutex(id));

  bool find;
  int sorted_index = Find_(key, find);
  if (!find) {
    // special situation, this node has split
    if (sorted_index == nr_entry) {
      LeafNode* next = GetNext(mem->BaseAddr());
      if (next && key >= next->min_key)
        return next->Delete(mem, mutex, key);
    }
    // key not exist
    return Status::DOES_NOT_EXIST;
  }

  uint64_t entry_index = GetSortedEntry_(sorted_index);

  DeleteSortedArray_(mem, sorted_index, entry_index);

  // Valid_();
  // PrintSortedArray();
  return Status::OK;
}

Status CLevel::LeafNode::Update(MemoryManagement* mem, Mutex& mutex,
                                uint64_t key, uint64_t value, Node* root) {
  assert(0);
}

Status CLevel::LeafNode::Get(MemoryManagement* mem, uint64_t key, uint64_t& value) const {
  // lock free
  bool find;
  int sorted_index = Find_(key, find);
  if (!find) {
    // special situation, this node has split
    if (sorted_index == nr_entry) {
      LeafNode* next = GetNext(mem->BaseAddr());
      if (next && key >= next->min_key)
        return next->Get(mem, key, value);
    }
    // key not exist
    return Status::DOES_NOT_EXIST;
  }

  int entry_index = GetSortedEntry_(sorted_index);
  value = entry[entry_index].value;
  return Status::OK;
}

bool CLevel::LeafNode::Scan(MemoryManagement* mem, uint64_t min_key,
                            uint64_t max_key, size_t max_size, size_t& size,
                            std::function<void(uint64_t,uint64_t)> callback) {
  bool find = false;
  uint64_t entry_key = 0;
  for (uint32_t i = 0; i < nr_entry; ++i) {
    entry_key = GetSortedKey_(i);
    if (entry_key < min_key)
      continue;
    if (size >= max_size || entry_key > max_key)
      return true;
    callback(entry_key, entry[GetSortedEntry_(i)].value);
    find = true;
    size++;
  }
  LeafNode* next_node = GetNext(mem->BaseAddr());
  if (find && next_node)
    return next_node->Scan_(mem, max_key, max_size, size, entry_key, callback);
  if (!find && next_node)
    return next_node->Scan(mem, min_key, max_key, max_size, size, callback);
  return false;
}

bool CLevel::LeafNode::Scan_(MemoryManagement* mem, uint64_t max_key,
                             size_t max_size, size_t& size, uint64_t last_seen,
                             std::function<void(uint64_t,uint64_t)> callback) {
  LeafNode* node = this;
  while (true) {
    uint64_t entry_key = 0;
    if (node->GetSortedKey_(0) > last_seen) {
      for (uint32_t i = 0; i < node->nr_entry; ++i) {
        entry_key = node->GetSortedKey_(i);
        if (size >= max_size || entry_key > max_key)
          return true;
        callback(entry_key, node->entry[node->GetSortedEntry_(i)].value);
        size++;
      }
    } else {
      // key[0] <= last_seen, special situation
#ifndef NDEBUG
      if (node->nr_entry != 0)
        LOG(Debug::WARNING, "scan special situation");
#endif // NDEBUG
      for (uint32_t i = 0; i < node->nr_entry; ++i) {
        entry_key = node->GetSortedKey_(i);
        if (size >= max_size || entry_key > max_key)
          return true;
        if (entry_key <= last_seen)
          continue;
        callback(entry_key, node->entry[node->GetSortedEntry_(i)].value);
        size++;
      }
    }
    node = node->GetNext(mem->BaseAddr());
    assert(node != this);
    if (!node)
      return false;
  }
}

// find sorted index which is bigger or equal to key
CLevel::Node CLevel::IndexNode::FindChild_(uint64_t key) const {
  int left = 0;
  int right = nr_entry - 1;
  // find first entry greater or equal than key
  while (left <= right) {
    int middle = (left + right) / 2;
    uint64_t mid_key = GetSortedKey_(middle);
    if (mid_key > key) { // TODO:
      right = middle - 1;
    } else if (mid_key <= key) {
      left = middle + 1;
    }
  }
  return right == -1 ? child[0] : child[GetSortedEntry_(right) + 1];
}

CLevel::IndexNode* CLevel::IndexNode::FindParent(uint64_t key, Node child) const {
  Node next_child = FindChild_(key);
  if (next_child == child)
    return const_cast<IndexNode*>(this);
  assert(next_child.IsIndex());
  return next_child.index()->FindParent(key, child);
}

CLevel::LeafNode* CLevel::IndexNode::FindLeafNode_(uint64_t key) const {
  assert(nr_entry > 0);
  Node child = FindChild_(key);
  if (child.IsLeaf())
    return child.leaf();
  else
    return child.index()->FindLeafNode_(key);
}

CLevel::IndexNode* CLevel::IndexNode::Split_(MemoryManagement* mem, Node* root) {
  assert(nr_entry == INDEX_ENTRYS);

  // get parent
  IndexNode* parent;
  if (root->index() == this) {
    // allocate new root
    parent = mem->NewIndexNode();
    parent->child[0] = this;
    mem->persist(parent, sizeof(*parent));
  } else {
    parent = root->index()->FindParent(GetSortedKey_(0), this);
  }

  // copy bigger half to new node
  IndexNode* new_node = mem->NewIndexNode();
  new_node->child[0] = child[GetSortedEntry_(INDEX_ENTRYS / 2) + 1];
  for (int i = 0; i < INDEX_ENTRYS / 2; ++i) {
    int idx = GetSortedEntry_(i + INDEX_ENTRYS - INDEX_ENTRYS / 2);
    new_node->keys[i] = keys[idx];
    new_node->child[i + 1] = child[idx + 1];
  }
  new_node->nr_entry = INDEX_ENTRYS / 2;
  mem->persist(new_node, sizeof(*new_node));

  // insert new node to parent
  parent->InsertChild(mem, GetSortedKey_(INDEX_ENTRYS / 2), new_node, root);

  // change root
  if (root->index() == this) {
    Node new_root(parent);
    *root = new_root;
    mem->persist(root, sizeof(*root));
  }

  // change metadata
  nr_entry = INDEX_ENTRYS / 2;
  mem->persist(&meta_data, sizeof(meta_data));

  return new_node;
}

bool CLevel::IndexNode::InsertChild(MemoryManagement* mem, uint64_t child_key,
                                    Node new_child, Node* root) {
  if (nr_entry >= INDEX_ENTRYS) {
    uint64_t mid_key = GetSortedKey_(INDEX_ENTRYS / 2);
    IndexNode* new_node = Split_(mem, root);
    assert(child_key != mid_key);
    if (child_key > mid_key)
      return new_node->InsertChild(mem, child_key, new_child, root);
  }

  int left = 0;
  int right = nr_entry - 1;
  while (left <= right) {
    int middle = (left + right) / 2;
    uint64_t mid_key = GetSortedKey_(middle);
    if (mid_key == child_key) {
      assert(0);
    } else if (mid_key < child_key) {
      left = middle + 1;
    } else {
      right = middle - 1;
    }
  }

  // left is the first entry bigger than leaf
  int entry_idx = GetFreeEntry_();
  PutChild_(mem, entry_idx, child_key, new_child);
  PutSortedArray_(mem, left, entry_idx);

  return true;
}

Status CLevel::IndexNode::Insert(MemoryManagement* mem, Mutex& mutex,
                                 uint64_t key, uint64_t value, Node* root) {
  auto leaf = FindLeafNode_(key);
  return leaf->Insert(mem, mutex, key, value, root);
}

Status CLevel::IndexNode::Update(MemoryManagement* mem, Mutex& mutex,
                                 uint64_t key, uint64_t value, Node* root) {
  auto leaf = FindLeafNode_(key);
  return leaf->Update(mem, mutex, key, value, root);
}

Status CLevel::IndexNode::Get(MemoryManagement* mem, uint64_t key, uint64_t& value) const {
  auto leaf = FindLeafNode_(key);
  return leaf->Get(mem, key, value);
}

Status CLevel::IndexNode::Delete(MemoryManagement* mem, Mutex& mutex, uint64_t key) {
  auto leaf = FindLeafNode_(key);
  return leaf->Delete(mem, mutex, key);
}

bool CLevel::IndexNode::Scan(MemoryManagement* mem, uint64_t min_key,
                             uint64_t max_key, size_t max_size, size_t& size,
                             std::function<void(uint64_t,uint64_t)> callback) {
  auto leaf = FindLeafNode_(min_key);
  return leaf->Scan(mem, min_key, max_key, max_size, size, callback);
}

} // namespace combotree