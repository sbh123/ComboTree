#pragma once

#include <cstdint>
#include <shared_mutex>
#include <atomic>
#include <memory>
#include <vector>
#include <libpmemobj++/persistent_ptr.hpp>
#include "status.h"
#include "clevel.h"
#include "slab.h"
#include "debug.h"

namespace combotree {

class ALevel;

class BLevel {
 public:
  BLevel(pmem::obj::pool_base pop, const std::vector<std::pair<uint64_t, uint64_t>>& kv);
  BLevel(pmem::obj::pool_base pop, std::shared_ptr<BLevel> old_blevel);
  BLevel(pmem::obj::pool_base pop);
  ~BLevel();

  Status Get(uint64_t key, uint64_t& value, uint64_t begin, uint64_t end) const {
    uint64_t idx = Find_(key, begin, end);
    return GetEntry_(idx)->Get(&locks_[idx], clevel_mem_, key, value);
  }

  Status Insert(uint64_t key, uint64_t value, uint64_t begin, uint64_t end) {
    uint64_t idx = Find_(key, begin, end);
    Status s = GetEntry_(idx)->Insert(&locks_[idx], clevel_mem_, clevel_slab_, key, value);
    if (s == Status::OK) {
      root_->size++;
    }
    return s;
  }

  Status Update(uint64_t key, uint64_t value, uint64_t begin, uint64_t end) {
    uint64_t idx = Find_(key, begin, end);
    return GetEntry_(idx)->Update(&locks_[idx], clevel_mem_, key, value);
  }

  Status Delete(uint64_t key, uint64_t begin, uint64_t end) {
    uint64_t idx = Find_(key, begin, end);
    Status s = GetEntry_(idx)->Delete(&locks_[idx], clevel_mem_, key);
    if (s == Status::OK) {
      root_->size--;
    }
    return s;
  }

  Status Get(uint64_t key, uint64_t& value) const {
    uint64_t end;
    if (is_expanding_.load(std::memory_order_acquire))
      end = expanding_entry_index_.load(std::memory_order_acquire) - 1;
    else
      end = EntrySize() - 1;
    return Get(key, value, 0, end);
  }

  Status Insert(uint64_t key, uint64_t value) {
    uint64_t end;
    if (is_expanding_.load(std::memory_order_acquire))
      end = expanding_entry_index_.load(std::memory_order_acquire) - 1;
    else
      end = EntrySize() - 1;
    return Insert(key, value, 0, end);
  }

  Status Update(uint64_t key, uint64_t value) {
    uint64_t end;
    if (is_expanding_.load(std::memory_order_acquire))
      end = expanding_entry_index_.load(std::memory_order_acquire) - 1;
    else
      end = EntrySize() - 1;
    return Update(key, value, 0, end);
  }

  Status Delete(uint64_t key) {
    uint64_t end;
    if (is_expanding_.load(std::memory_order_acquire))
      end = expanding_entry_index_.load(std::memory_order_acquire) - 1;
    else
      end = EntrySize() - 1;
    return Delete(key, 0, end);
  }

  Status Scan(uint64_t min_key, uint64_t max_key, size_t max_size, size_t& size,
              callback_t callback, void* arg);

  uint64_t MinEntryKey() const;
  uint64_t MaxEntryKey() const;

  uint64_t Size() const { return root_->size.load(); }
  uint64_t EntrySize() const { return root_->nr_entry.load(); }

  uint64_t GetKey(uint64_t index) const {
    return in_mem_key_[index];
  }

  void Expansion(std::shared_ptr<BLevel> old_blevel, std::atomic<uint64_t>& min_key,
                 std::atomic<uint64_t>& max_key);

  friend class ALevel;

 private:
  struct Entry {
    uint64_t key;

    union {
      uint64_t value;
      struct {
        uint64_t ptr  : 62;
        uint64_t type :  2;
      };
    };

    enum Type : uint64_t {
      ENTRY_INVALID = 0x00UL,
      ENTRY_VALUE   = 0x01UL,
      ENTRY_CLEVEL  = 0x02UL,
      ENTRY_NONE    = 0x03UL,
    };

    Entry() : key(0), value(0) {}
    Status Get(std::shared_mutex* mutex, CLevel::MemoryManagement* mem, uint64_t pkey, uint64_t& pvalue) const;
    Status Insert(std::shared_mutex* mutex, CLevel::MemoryManagement* mem, Slab<CLevel>* clevel_slab, uint64_t pkey, uint64_t pvalue);
    Status Update(std::shared_mutex* mutex, CLevel::MemoryManagement* mem, uint64_t pkey, uint64_t pvalue);
    Status Delete(std::shared_mutex* mutex, CLevel::MemoryManagement* mem, uint64_t pkey);

    void SetKey(CLevel::MemoryManagement* mem, uint64_t new_key) {
      key = new_key;
      mem->persist(&key, sizeof(key));
    }

    void SetValue(CLevel::MemoryManagement* mem, uint64_t new_value) {
      assert((new_value >> 62) == 0);
      value = (Type::ENTRY_VALUE << 62) | new_value;
      mem->persist(&value, sizeof(value));
    }

    void SetCLevel(CLevel::MemoryManagement* mem, CLevel* new_clevel) {
      uint64_t offset = reinterpret_cast<uint64_t>(new_clevel) - mem->BaseAddr();
      value = (Type::ENTRY_CLEVEL << 62) | offset;
      mem->persist(&value, sizeof(value));
    }

    void SetNone(CLevel::MemoryManagement* mem) {
      type = Type::ENTRY_NONE;
      mem->persist(&value, sizeof(value));
    }

    void SetInvalid(CLevel::MemoryManagement* mem) {
      type = Type::ENTRY_INVALID;
      mem->persist(&value, sizeof(value));
    }

    static CLevel* GetClevel(CLevel::MemoryManagement* mem, uint64_t data) {
      return reinterpret_cast<CLevel*>(GetValue(data) + mem->BaseAddr());
    }

    static uint64_t GetValue(uint64_t data) { return data & 0x3FFFFFFFFFFFFFFFUL; }
    static uint64_t GetType(uint64_t data) { return data >> 62; }

    bool IsNone() const { return type == Type::ENTRY_NONE; }
    bool IsValue() const { return type == Type::ENTRY_VALUE; }
    bool IsClevel() const { return type == Type::ENTRY_CLEVEL; }
    bool IsInvalid() const { return type == Type::ENTRY_INVALID; }
  };

  pmem::obj::pool_base pop_;
  uint64_t base_addr_;
  Slab<CLevel>* clevel_slab_;
  CLevel::MemoryManagement* clevel_mem_;

  struct Root {
    pmem::obj::persistent_ptr<Entry[]> entry;
    std::atomic<uint64_t> nr_entry;
    std::atomic<uint64_t> size;
  };

  pmem::obj::persistent_ptr<Root> root_;
  Entry* in_mem_entry_;
  uint64_t* in_mem_key_;
  std::shared_mutex* locks_;
  std::atomic<uint64_t> expanding_entry_index_;
  std::atomic<bool> is_expanding_;

  Entry* GetEntry_(int index) const { return &in_mem_entry_[index]; }
  uint64_t Find_(uint64_t key, uint64_t begin, uint64_t end) const;
  void ExpandAddEntry_(uint64_t key, uint64_t value, size_t& size);

  static void ExpansionCallback1_(uint64_t key, uint64_t value, void* arg);
  static void ExpansionCallback2_(uint64_t key, uint64_t value, void* arg);
};

} // namespace combotree