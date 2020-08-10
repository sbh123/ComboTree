#pragma once

#include <cstdint>
#include <shared_mutex>
#include <atomic>
#include <memory>
#include <libpmemobj++/persistent_ptr.hpp>
#include "combotree/iterator.h"
#include "status.h"
#include "clevel.h"
#include "debug.h"

namespace combotree {

class ALevel;

class BLevel {
 public:
  class Iter;

  BLevel(pmem::obj::pool_base pop, Iterator* iter, uint64_t size);
  BLevel(pmem::obj::pool_base pop, std::shared_ptr<BLevel> old_blevel);
  BLevel(pmem::obj::pool_base pop);
  ~BLevel();

  Status Get(uint64_t key, uint64_t& value, uint64_t begin, uint64_t end) const {
    uint64_t idx = Find_(key, begin, end);
    return GetEntry_(idx)->Get(&locks_[idx], base_addr_, key, value);
  }

  Status Insert(uint64_t key, uint64_t value, uint64_t begin, uint64_t end) {
    uint64_t idx = Find_(key, begin, end);
    Status s = GetEntry_(idx)->Insert(&locks_[idx], base_addr_, pop_, key, value);
    if (s == Status::OK) {
      root_->size++;
    }
    return s;
  }

  Status Update(uint64_t key, uint64_t value, uint64_t begin, uint64_t end) {
    uint64_t idx = Find_(key, begin, end);
    return GetEntry_(idx)->Update(&locks_[idx], base_addr_, key, value);
  }

  Status Delete(uint64_t key, uint64_t begin, uint64_t end) {
    uint64_t idx = Find_(key, begin, end);
    Status s = GetEntry_(idx)->Delete(&locks_[idx], base_addr_, key);
    if (s == Status::OK) {
      root_->size--;
    }
    return s;
  }

  Status Get(uint64_t key, uint64_t& value) const {
    uint64_t end;
    if (is_expanding_.load())
      end = std::min<uint64_t>(root_->size, EntrySize()) - 1;
    else
      end = EntrySize() - 1;
    return Get(key, value, 0, end);
  }

  Status Insert(uint64_t key, uint64_t value) {
    uint64_t end;
    if (is_expanding_.load())
      end = std::min<uint64_t>(root_->size, EntrySize()) - 1;
    else
      end = EntrySize() - 1;
    return Insert(key, value, 0, end);
  }

  Status Update(uint64_t key, uint64_t value) {
    uint64_t end;
    if (is_expanding_.load())
      end = std::min<uint64_t>(root_->size, EntrySize()) - 1;
    else
      end = EntrySize() - 1;
    return Update(key, value, 0, end);
  }

  Status Delete(uint64_t key) {
    uint64_t end;
    if (is_expanding_.load())
      end = std::min<uint64_t>(root_->size, EntrySize()) - 1;
    else
      end = EntrySize() - 1;
    return Delete(key, 0, end);
  }

  uint64_t MinKey() const;
  uint64_t MaxKey() const;

  uint64_t MinEntryKey() const;
  uint64_t MaxEntryKey() const;

  uint64_t Size() const { return root_->size; }
  uint64_t EntrySize() const { return root_->nr_entry; }

  uint64_t GetKey(uint64_t index) const {
    return in_mem_key_[index];
  }

  void Expansion(std::shared_ptr<BLevel> old_blevel, std::atomic<uint64_t>& min_key,
                 std::atomic<uint64_t>& max_key);

  Iterator* begin();
  Iterator* end();

  friend class ALevel;

 private:
  struct Entry {
    uint64_t key;
    union {
      uint64_t value;
      uint64_t clevel;
      uint64_t type;
    };

    enum Type : uint64_t {
      ENTRY_UNVALID = 0x0000000000000000UL,
      ENTRY_VALUE   = 0x4000000000000000UL,
      ENTRY_CLEVEL  = 0x8000000000000000UL,
      ENTRY_NONE    = 0xC000000000000000UL,
    };

    Entry() : type(Type::ENTRY_NONE) {}
    Status Get(std::shared_mutex* mutex, uint64_t base_addr, uint64_t pkey, uint64_t& pvalue) const;
    Status Insert(std::shared_mutex* mutex, uint64_t base_addr, pmem::obj::pool_base& pop, uint64_t pkey, uint64_t pvalue);
    Status Update(std::shared_mutex* mutex, uint64_t base_addr, uint64_t pkey, uint64_t pvalue);
    Status Delete(std::shared_mutex* mutex, uint64_t base_addr, uint64_t pkey);

    const static uint64_t type_mask_  = 0xC000000000000000UL;
    const static uint64_t value_mask_ = 0x3FFFFFFFFFFFFFFFUL;

    void SetKey(uint64_t new_key) {
      key = new_key;
    }

    void SetValue(uint64_t new_value) {
      assert((new_value & value_mask_) == new_value);
      value = GetType() | (new_value & value_mask_);
    }

    void SetClevel(CLevel* new_clevel, uint64_t base_addr) {
      uint64_t offset = (uint64_t)new_clevel - base_addr;
      assert((offset & value_mask_) == offset);
      clevel = GetType() | (offset & value_mask_);
    }

    CLevel* GetClevel(uint64_t base_addr) const {
      return reinterpret_cast<CLevel*>((clevel & value_mask_) + base_addr);
    }

    uint64_t GetType() const {
      return type & type_mask_;
    }

    uint64_t GetValue() const {
      return value & value_mask_;
    }

    uint64_t GetKey() const {
      return key;
    }

    bool IsNone() const {
      return GetType() == Type::ENTRY_NONE;
    }

    bool IsValue() const {
      return GetType() == Type::ENTRY_VALUE;
    }

    bool IsClevel() const {
      return GetType() == Type::ENTRY_CLEVEL;
    }

    bool IsUnValid() const {
      return GetType() == Type::ENTRY_UNVALID;
    }

    void SetTypeNone() {
      type = GetValue() | Type::ENTRY_NONE;
    }

    void SetTypeValue() {
      type = GetValue() | Type::ENTRY_VALUE;
    }

    void SetTypeClevel() {
      type = GetValue() | Type::ENTRY_CLEVEL;
    }

    void SetTypeUnValid() {
      type = GetValue() | Type::ENTRY_UNVALID;
    }
  };

  pmem::obj::pool_base pop_;
  uint64_t base_addr_;

  struct Root {
    pmem::obj::persistent_ptr<Entry[]> entry;
    uint64_t nr_entry;
    std::atomic<uint64_t> size;
  };

  pmem::obj::persistent_ptr<Root> root_;
  Entry* in_mem_entry_;
  uint64_t* in_mem_key_;
  std::shared_mutex* locks_;
  std::atomic<bool> is_expanding_;

  Entry* GetEntry_(int index) const {
    return &in_mem_entry_[index];
  }

  uint64_t Find_(uint64_t key, uint64_t begin, uint64_t end) const;

  void ExpandAddEntry_(uint64_t& index, uint64_t key, uint64_t value);
};

class BLevel::Iter : public Iterator {
 public:
  explicit Iter(BLevel* blevel)
      : blevel_(blevel), entry_index_(0), clevel_iter_(nullptr), locked_(false)
  {}

  ~Iter() { if (locked_) Unlock_(); }

  bool Valid() const {
    if (entry_index_ < 0 || entry_index_ >= blevel_->EntrySize())
      return false;
    if (blevel_->GetEntry_(entry_index_)->IsUnValid())
      return true;
    if (EntryType_() == BLevel::Entry::Type::ENTRY_NONE)
      return false;
    else if (EntryType_() == BLevel::Entry::Type::ENTRY_CLEVEL) {
      if (clevel_iter_ == nullptr)
        clevel_iter_ = GetClevel_()->begin();
      return clevel_iter_->Valid() && !clevel_iter_->End();
    }
    else if (EntryType_() == BLevel::Entry::Type::ENTRY_VALUE)
      return true;
    assert(0);
    return false;
  }

  bool Begin() const {
    if (entry_index_ == 0) {
      if (entry_type_ == BLevel::Entry::Type::ENTRY_CLEVEL)
        return clevel_iter_->Begin();
      else
        return true;
    }
    return false;
  }

  bool End() const {
    if (entry_index_ >= blevel_->EntrySize()) {
      return true;
    } else if (entry_index_ == blevel_->EntrySize() - 1) {
      if (EntryType_() == BLevel::Entry::Type::ENTRY_CLEVEL) {
        if (clevel_iter_ == nullptr)
          clevel_iter_ = GetClevel_()->begin();
        return clevel_iter_->End();
      }
    }
    return false;
  }

  void SeekToFirst() {
    entry_index_ = 0;
    Lock_();
    entry_type_ = EntryType_();
    if (entry_type_ == BLevel::Entry::Type::ENTRY_CLEVEL) {
      clevel_iter_ = GetClevel_()->begin();
      if (clevel_iter_->End())
        Next();
      return;
    } else if (entry_type_ == BLevel::Entry::Type::ENTRY_VALUE) {
      return;
    } else if (entry_type_ == BLevel::Entry::Type::ENTRY_NONE) {
      Next();
    }
  }

  void SeekToLast() {
    entry_index_ = blevel_->EntrySize() - 1;
    Lock_();
    while (EntryType_() == BLevel::Entry::Type::ENTRY_NONE) {
      Unlock_();
      entry_index_--;
      Lock_();
    }
    entry_type_ = EntryType_();
    if (entry_type_ == BLevel::Entry::Type::ENTRY_CLEVEL) {
      clevel_iter_ = GetClevel_()->end();
    }
  }

  void Seek(uint64_t target, uint64_t begin, uint64_t end) {
    Unlock_();
    entry_index_ = blevel_->Find_(target, begin, end);
    Lock_();
    entry_type_ = EntryType_();
    if (entry_type_ == BLevel::Entry::Type::ENTRY_VALUE) {
      if (blevel_->GetKey(entry_index_) < target)
        Next();
    } else if (entry_type_ == BLevel::Entry::Type::ENTRY_CLEVEL) {
      clevel_iter_ = GetClevel_()->begin();
      clevel_iter_->Seek(target);
      if (clevel_iter_->End())
        Next();
    } else if (entry_type_ == BLevel::Entry::Type::ENTRY_NONE) {
      Next();
    }
  }

  void Seek(uint64_t target) {
    Seek(target, 0, blevel_->EntrySize() - 1);
  }

  void Next() {
    if (entry_type_ == BLevel::Entry::Type::ENTRY_CLEVEL) {
      clevel_iter_->Next();
      if (!clevel_iter_->End())
        return;
    }
    NextEntry_();
  }

  void Prev() { assert(0); }

  uint64_t key() const {
    switch (entry_type_) {
      case BLevel::Entry::Type::ENTRY_CLEVEL:
        return clevel_iter_->key();
      case BLevel::Entry::Type::ENTRY_NONE:
        assert(0);
        return 0;
      case BLevel::Entry::Type::ENTRY_VALUE:
        return blevel_->GetKey(entry_index_);
      default:
        assert(0);
        return 0;
    }
  }

  uint64_t value() const {
    switch (entry_type_) {
      case BLevel::Entry::Type::ENTRY_CLEVEL:
        return clevel_iter_->value();
      case BLevel::Entry::Type::ENTRY_NONE:
        assert(0);
        return 0;
      case BLevel::Entry::Type::ENTRY_VALUE:
        return blevel_->GetEntry_(entry_index_)->GetValue();
      default:
        assert(0);
        return 0;
    }
  }

 private:
  BLevel* blevel_;
  uint64_t entry_index_;
  uint64_t begin_entry_index_;
  mutable Iterator* clevel_iter_;
  uint64_t entry_type_;
  bool locked_;

  void Lock_() {
    assert(locked_ == false);
    blevel_->locks_[entry_index_].lock();
    locked_ = true;
  }

  void Unlock_() {
    assert(locked_ == true);
    blevel_->locks_[entry_index_].unlock();
    locked_ = false;
  }

  uint64_t EntryType_() const {
    return blevel_->GetEntry_(entry_index_)->GetType();
  }

  void NextEntry_() {
    Unlock_();
    if (clevel_iter_)
      delete clevel_iter_;
    clevel_iter_ = nullptr;
    entry_index_++;
    if (entry_index_ >= blevel_->EntrySize())
      return;
    Lock_();
    while (!End() && !Valid()) {
      Unlock_();
      if (clevel_iter_)
        delete clevel_iter_;
      clevel_iter_ = nullptr;
      entry_index_++;
      Lock_();
    }
    if (End())
      return;
    entry_type_ = EntryType_();
  }

  CLevel* GetClevel_() const {
    return blevel_->GetEntry_(entry_index_)->GetClevel(blevel_->base_addr_);
  }

};

} // namespace combotree