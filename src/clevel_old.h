#pragma once

#include <cstdint>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <vector>
#include <mutex>
#include <atomic>
#include "slab.h"
#include "status.h"
#include "combotree_config.h"
#include "debug.h"

namespace combotree {

#define LEAF_ENTRYS   CLEVEL_LEAF_ENTRY
#define INDEX_ENTRYS  CLEVEL_INDEX_ENTRY

typedef void (*callback_t)(uint64_t,uint64_t,void*);

class BLevel;

class CLevel {
 public:
  class MemControl;

  void InitLeaf(MemControl* mem);

  Status Put(MemControl* mem, uint64_t key, uint64_t value);
  Status Update(MemControl* mem, uint64_t key, uint64_t value);
  Status Delete(MemControl* mem, uint64_t key);
  Status Get(MemControl* mem, uint64_t key, uint64_t& value) const;
  bool Scan(MemControl* mem, uint64_t min_key, uint64_t max_key, size_t max_size, size_t& size,
            callback_t callback, void* arg);
  bool Scan(MemControl* mem, uint64_t max_key, size_t max_size,
            size_t& size, callback_t callback, void* arg);

  friend BLevel;

  struct LeafNode;
 private:
  struct IndexNode;

  class Mutex {
   private:
    static const int GROUP_SIZE = 32;
    std::vector<std::mutex*> leaf_mutex;
    std::mutex global_mutex;
    std::mutex expand_mutex;
    std::atomic<uint64_t> id_pool;

   public:
    std::mutex& GetLeafMutex(uint64_t id) {
      return leaf_mutex[id / GROUP_SIZE][id % GROUP_SIZE];
    }

    std::mutex& GetGlobalMutex() {
      return global_mutex;
    }

    uint64_t AllocateId() {
      uint64_t new_id = id_pool.fetch_add(1);
      unsigned dim1 = new_id / GROUP_SIZE;
      if (dim1 + 1 > leaf_mutex.size()) {
        std::lock_guard<std::mutex> lock(expand_mutex);
        while (dim1 + 1 > leaf_mutex.size())
          leaf_mutex.emplace_back(new std::mutex[GROUP_SIZE]);
      }
      return new_id;
    }
  };

  struct Node {
    enum Type : uint64_t {
      LEAF  = 0,
      INDEX = 1,
    };

    union {
      uint64_t data;
      struct {
        uint64_t ptr  : 63;
        uint64_t type :  1;
      };
    };

    Node() : data(0) {}
    Node(Type type, void* ptr) : ptr(reinterpret_cast<uint64_t>(ptr)), type(type) {}
    Node(IndexNode* ptr) : ptr(reinterpret_cast<uint64_t>(ptr)), type(Type::INDEX) {}
    Node(LeafNode* ptr) : ptr(reinterpret_cast<uint64_t>(ptr)), type(Type::LEAF) {}

    bool operator==(Node& b) const volatile { return data == b.data; }
    bool IsLeaf() const volatile { return type == Type::LEAF; }
    bool IsIndex() const volatile { return type == Type::INDEX; }
    IndexNode* index() const volatile { return reinterpret_cast<IndexNode*>(ptr); }
    LeafNode* leaf() const volatile { return reinterpret_cast<LeafNode*>(ptr); }
  };

  Node root_;
  LeafNode* head_;
  Mutex mutex_;
};

// allocate and persist clevel node
class CLevel::MemControl {
 public:
  MemControl(pmem::obj::pool_base pop, Slab<LeafNode>* leaf_slab)
      : pop_(pop), leaf_slab_(leaf_slab), base_addr_(leaf_slab_->BaseAddr()) {}

  LeafNode* NewLeafNode() {
    return leaf_slab_->Allocate();
  }

  IndexNode* NewIndexNode() {
    pmem::obj::persistent_ptr<IndexNode> tmp;
    pmem::obj::make_persistent_atomic<IndexNode>(pop_, tmp);
    return tmp.get();
  }

  uint64_t BaseAddr() const {
    return base_addr_;
  }

  void persist(const void* addr, size_t len) {
    pop_.persist(addr, len);
  }

  void memcpy_persist(void* dest, const void* src, size_t len) {
    pop_.memcpy_persist(dest, src, len);
  }

 private:
  pmem::obj::pool_base pop_;
  Slab<LeafNode>* leaf_slab_;
  uint64_t base_addr_;
};

struct CLevel::LeafNode {
  LeafNode() : id(0), next(0), min_key(0), nr_entry(0),
      sorted_array(0x0123456789ABCDEUL) {}

  uint64_t id;
  uint64_t next;
  uint64_t min_key;

  union {
    struct {
      uint64_t nr_entry     :  4;
      uint64_t sorted_array : 60;
    };
    uint64_t meta_data;
  };

  struct Entry {
    Entry() : key(0), value(0) {}
    uint64_t key;
    uint64_t value;
  } entry[LEAF_ENTRYS];

  Status Put(MemControl* mem, Mutex& mutex, uint64_t key, uint64_t value, Node* root);
  Status Update(MemControl* mem, Mutex& mutex, uint64_t key, uint64_t value, Node* root);
  Status Get(MemControl* mem, uint64_t key, uint64_t& value) const;
  Status Delete(MemControl* mem, Mutex& mutex, uint64_t key);
  bool Scan(MemControl* mem, uint64_t min_key, uint64_t max_key,
            size_t max_size, size_t& size, callback_t callback, void* arg);
  bool Scan_(MemControl* mem, uint64_t max_key, size_t max_size,
             size_t& size, uint64_t last_seen, callback_t callback, void* arg);

  void PrintSortedArray() const;

  LeafNode* GetNext(uint64_t base_addr) const {
    return next == 0x00UL ? nullptr :
        reinterpret_cast<LeafNode*>((next & 0x7FFFFFFFFFFFFFFFUL) + base_addr);
  }

  void SetNext(uint64_t base_addr, LeafNode* next_ptr) {
    next = (reinterpret_cast<uint64_t>(next_ptr) - base_addr) | 0x8000000000000000UL;
  }

  friend BLevel;

 private:
  int GetFreeEntry_() const {
    assert(nr_entry < LEAF_ENTRYS);
    uint64_t mask = 0x0FUL << ((14 - nr_entry) * 4);
    return (sorted_array & mask) >> ((14 - nr_entry) * 4);
  }

  void PutSortedArray_(MemControl* mem, int position, int index) {
    assert(position >= 0 && position < LEAF_ENTRYS);
    assert(index >= 0 && index < LEAF_ENTRYS);
    assert(nr_entry < LEAF_ENTRYS);
    uint64_t unchange_mask = (~0x00UL << ((15 - position) * 4)) |
                             ~(~0x00UL << ((14 - nr_entry) * 4));
    uint64_t after_mask = (~unchange_mask << 4) & ~unchange_mask;

    uint64_t unchange = unchange_mask & sorted_array;
    uint64_t after = (after_mask & sorted_array) >> 4;
    uint64_t new_item = (uint64_t)index << ((14 - position) * 4);

    uint64_t new_sorted_array = unchange | after | new_item;
    meta_data = (new_sorted_array << 4) | (nr_entry + 1);
    mem->persist(&meta_data, sizeof(meta_data));
  }

  void DeleteSortedArray_(MemControl* mem, int position, int index) {
    assert(position >= 0 && position < LEAF_ENTRYS);
    assert(nr_entry <= LEAF_ENTRYS);
    uint64_t unchange_mask = (~0x00UL << ((15 - position) * 4)) |
                             ~(~0x00UL << ((15 - nr_entry) * 4));
    uint64_t after_mask = (~unchange_mask >> 4) & ~unchange_mask;

    uint64_t unchange = unchange_mask & sorted_array;
    uint64_t after = (after_mask & sorted_array) << 4;
    uint64_t new_free = (uint64_t)index << ((15 - nr_entry) * 4);

    uint64_t new_sorted_array = unchange | after | new_free;
    meta_data = (new_sorted_array << 4) | (nr_entry - 1);
    mem->persist(&meta_data, sizeof(meta_data));
  }

  void PutEntry_(MemControl* mem, int index, uint64_t key, uint64_t value) {
    Entry tmp;
    tmp.key = key;
    tmp.value = value;
    mem->memcpy_persist(&entry[index], &tmp, sizeof(tmp));
  }

  LeafNode* Split_(MemControl* mem, Mutex& mutex, Node* root);

  void Valid_();

  /*
   * find entry index which is equal or bigger than key
   *
   * @find   if equal, return true
   * @return entry index
   */
  int Find_(uint64_t key, bool& find) const;

  /*
   * get entry index in sorted array
   */
  int GetSortedEntry_(int sorted_index) const {
    uint64_t mask = (uint64_t)0x0FUL << ((14 - sorted_index) * 4);
    return (sorted_array & mask) >> ((14 - sorted_index) * 4);
  }

  uint64_t GetSortedKey_(int idx) const {
    return entry[GetSortedEntry_(idx)].key;
  }
};

// Index Node
struct CLevel::IndexNode {
  union {
    struct {
      uint64_t nr_entry     :  4;
      uint64_t sorted_array : 60;
    };
    uint64_t meta_data;
  };

  uint64_t keys[INDEX_ENTRYS];
  Node child[INDEX_ENTRYS + 1];

  IndexNode() : nr_entry(0), sorted_array(0x0123456789ABCDEUL) {}

  Status Put(MemControl* mem, Mutex& mutex, uint64_t key, uint64_t value, Node* root);
  Status Update(MemControl* mem, Mutex& mutex,uint64_t key, uint64_t value, Node* root);
  Status Get(MemControl* mem, uint64_t key, uint64_t& value) const;
  Status Delete(MemControl* mem, Mutex& mutex,uint64_t key);
  bool Scan(MemControl* mem, uint64_t min_key, uint64_t max_key,
            size_t max_size, size_t& size, callback_t callback, void* arg);

  bool InsertChild(MemControl* mem, uint64_t child_key, Node child, Node* root);
  IndexNode* FindParent(uint64_t key, Node child) const;

 private:
  IndexNode* Split_(MemControl* mem, Node* root);

  Node FindChild_(uint64_t key) const;
  LeafNode* FindLeafNode_(uint64_t key) const;

  void PutChild_(MemControl* mem, int index, uint64_t key, Node new_child) {
    keys[index] = key;
    child[index + 1] = new_child;
    mem->persist(&keys[index], sizeof(key));
    mem->persist(&child[index + 1], sizeof(new_child));
  }

  void PutSortedArray_(MemControl* mem, int position, int index) {
    assert(position >= 0 && position < INDEX_ENTRYS);
    assert(index >= 0 && index < INDEX_ENTRYS);
    assert(nr_entry < INDEX_ENTRYS);
    uint64_t unchange_mask = (~0x00UL << ((15 - position) * 4)) |
                             ~(~0x00UL << ((14 - nr_entry) * 4));
    uint64_t after_mask = (~unchange_mask << 4) & ~unchange_mask;

    uint64_t unchange = unchange_mask & sorted_array;
    uint64_t after = (after_mask & sorted_array) >> 4;
    uint64_t new_item = (uint64_t)index << ((14 - position) * 4);

    uint64_t new_sorted_array = unchange | after | new_item;
    meta_data = (new_sorted_array << 4) | (nr_entry + 1);
    mem->persist(&meta_data, sizeof(meta_data));
  }

  void DeleteSortedArray_(MemControl* mem, int position, int index) {
    assert(position >= 0 && position < INDEX_ENTRYS);
    assert(nr_entry < INDEX_ENTRYS);
    uint64_t unchange_mask = (~0x00UL << ((15 - position) * 4)) |
                             ~(~0x00UL << ((15 - nr_entry) * 4));
    uint64_t after_mask = (~unchange_mask >> 4) & ~unchange_mask;

    uint64_t unchange = unchange_mask & sorted_array;
    uint64_t after = (after_mask & sorted_array) << 4;
    uint64_t new_free = (uint64_t)index << ((15 - nr_entry) * 4);

    uint64_t new_sorted_array = unchange | after | new_free;
    meta_data = (new_sorted_array << 4) | (nr_entry - 1);
    mem->persist(&meta_data, sizeof(meta_data));
  }

  int GetFreeEntry_() const {
    assert(nr_entry < INDEX_ENTRYS);
    uint64_t mask = 0x0FUL << ((14 - nr_entry) * 4);
    return (sorted_array & mask) >> ((14 - nr_entry) * 4);
  }

  /*
   * get entry index in sorted array
   */
  int GetSortedEntry_(int sorted_index) const {
    uint64_t mask = (uint64_t)0x0FUL << ((14 - sorted_index) * 4);
    return (sorted_array & mask) >> ((14 - sorted_index) * 4);
  }

  uint64_t GetSortedKey_(int idx) const {
    return keys[GetSortedEntry_(idx)];
  }
};

} // namespace combotree