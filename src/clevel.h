#pragma once

#include <cstdint>
#include "kvbuffer.h"
#include "combotree_config.h"
#include "debug.h"
#include "pmem.h"

namespace combotree {

class BLevel;

// B+ tree
class __attribute__((packed)) CLevel {
 public:
  class MemControl;

 private:
  struct __attribute__((aligned(64))) Node {
    enum class Type : uint8_t {
      INVALID,
      INDEX,
      LEAF,
    };

    Type type;
    uint8_t __padding[7];           // not used
    union {
      // uint48_t
      uint8_t next[6];              // used when type == LEAF. LSB == 1 means NULL
      uint8_t first_child[6];       // used when type == INDEX.
    };
    union {
      // contains 2 bytes meta
      KVBuffer<48+64,8> leaf_buf;   // used when type == LEAF
      KVBuffer<48+64,6> index_buf;  // used when type == INDEX
    };

    Node* Put(MemControl* mem, uint64_t key, uint64_t value, Node* parent);
    bool Get(MemControl* mem, uint64_t key, uint64_t& value) const;
    bool Delete(MemControl* mem, uint64_t key, uint64_t* value);

    ALWAYS_INLINE Node* GetChild(int pos, uint64_t base_addr) const {
      if (pos == 0)
        return (Node*)(((*(uint64_t*)first_child) & 0x0000FFFFFFFFFFFFUL) + base_addr);
      else
        return (Node*)((index_buf.value(pos-1) & 0x0000FFFFFFFFFFFFUL) + base_addr);
    }

    ALWAYS_INLINE Node* GetNext(uint64_t base_addr) const {
      return (next[0] & 0x1) ? (Node*)((*(uint64_t*)next)+base_addr) : nullptr;
    }

    ALWAYS_INLINE void SetNext(uint64_t base_addr, Node* next_ptr) {
      uint64_t tmp = (uint64_t)next_ptr - base_addr;
      memcpy(next, &tmp, sizeof(next));
    }

    ALWAYS_INLINE const Node* FindLeaf(MemControl* mem, uint64_t key) const {
      const Node* node = this;
      while (node->type != Type::LEAF) {
        assert(node->type != Type::INVALID);
        bool exist;
        int pos = node->index_buf.Find(key, exist);
        node = node->GetChild(exist ? pos + 1 : pos, mem->BaseAddr());
      }
      return node;
    }

    friend CLevel::MemControl;
  };

  static_assert(sizeof(CLevel::Node) == 128, "sizeof(CLevel::Node) != 128");

 public:
  // allocate and persist clevel node
  class MemControl {
   public:
    MemControl(void* base_addr)
      : base_addr_((uint64_t)base_addr), cur_addr_(base_addr)
    {}

    CLevel::Node* NewNode(Node::Type type, int suffix_len) {
      assert(suffix_len > 0 && suffix_len <= 8);
      CLevel::Node* ret = (CLevel::Node*)cur_addr_;
      cur_addr_ = (uint8_t*)cur_addr_ + sizeof(CLevel::Node);
      ret->type = type;
      ret->leaf_buf.suffix_bytes = suffix_len;
      ret->leaf_buf.prefix_bytes = 8 - suffix_len;
      ret->leaf_buf.entries = 0;
      ret->leaf_buf.max_entries = ret->leaf_buf.MaxEntries();
      return ret;
    }

    uint64_t BaseAddr() const {
      return base_addr_;
    }

   private:
    uint64_t base_addr_;
    void* cur_addr_;
  };

  CLevel();
  ALWAYS_INLINE bool HasSetup() const { return !(root_[0] & 1); };
  void Setup(MemControl* mem, int suffix_len);
  void Setup(MemControl* mem, KVBuffer<48+64,8>& buf);
  bool Put(MemControl* mem, uint64_t key, uint64_t value);

  ALWAYS_INLINE bool Get(MemControl* mem, uint64_t key, uint64_t& value) const {
    return root(mem->BaseAddr())->Get(mem, key, value);
  }

  ALWAYS_INLINE bool Delete(MemControl* mem, uint64_t key, uint64_t* value) {
    return root(mem->BaseAddr())->Delete(mem, key, value);
  }

 private:
  struct Node;

  uint8_t root_[6]; // uint48_t, LSB == 1 means NULL

  ALWAYS_INLINE Node* root(uint64_t base_addr) const {
    return (Node*)(((*(uint64_t*)root_) & 0x0000FFFFFFFFFFFFUL) + base_addr);
  }
};

static_assert(sizeof(CLevel) == 6, "sizeof(CLevel) != 6");

} // namespace combotree