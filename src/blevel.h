#pragma once

#include <cstdint>
#include <shared_mutex>
#include <atomic>
#include <libpmemobj++/persistent_ptr.hpp>
#include "iterator.h"
#include "clevel.h"
#include "debug.h"

namespace combotree {

class ALevel;

class BLevel {
 public:
  class Iter;

  BLevel(pmem::obj::pool_base pop, Iterator* iter, uint64_t size);
  BLevel(pmem::obj::pool_base pop);
  ~BLevel();

  bool Get(uint64_t key, uint64_t& value, uint64_t begin = 0, uint64_t end = UINT64_MAX) const {
    if (end == UINT64_MAX) end = EntrySize() - 1;
    uint64_t idx = Find_(key, begin, end);
    return GetEntry_(idx)->Get(&locks_[idx], key, value);
  }

  bool Insert(uint64_t key, uint64_t value, uint64_t begin = 0, uint64_t end = UINT64_MAX) {
    if (end == UINT64_MAX) end = EntrySize() - 1;
    uint64_t idx = Find_(key, begin, end);
    bool res = GetEntry_(idx)->Insert(&locks_[idx], pop_, key, value);
    if (res) {
      root_->size++;
    }
    return res;
  }

  bool Update(uint64_t key, uint64_t value, uint64_t begin = 0, uint64_t end = UINT64_MAX) {
    if (end == UINT64_MAX) end = EntrySize() - 1;
    uint64_t idx = Find_(key, begin, end);
    return GetEntry_(idx)->Update(&locks_[idx], key, value);
  }

  bool Delete(uint64_t key, uint64_t begin = 0, uint64_t end = UINT64_MAX) {
    if (end == UINT64_MAX) end = EntrySize() - 1;
    uint64_t idx = Find_(key, begin, end);
    bool res = GetEntry_(idx)->Delete(&locks_[idx], key);
    if (res) {
      root_->size--;
    }
    return res;
  }

  uint64_t MinKey() const;
  uint64_t MaxKey() const;

  uint64_t MinEntryKey() const;
  uint64_t MaxEntryKey() const;

  uint64_t Size() const { return root_->size; }
  uint64_t EntrySize() const { return root_->nr_entry; }

  Iterator* begin();
  Iterator* end();

  friend class ALevel;

 private:
  struct Entry {
    enum class Type {
      ENTRY_NONE,
      ENTRY_VALUE,
      ENTRY_CLVEL,
    } type;
    uint64_t key;
    union {
      uint64_t value;
      pmem::obj::persistent_ptr<CLevel> clevel;
    };

    Entry() : type(Type::ENTRY_NONE) {}
    bool Get(std::shared_mutex* mutex, uint64_t pkey, uint64_t& pvalue) const;
    bool Insert(std::shared_mutex* mutex, pmem::obj::pool_base& pop, uint64_t pkey, uint64_t pvalue);
    bool Update(std::shared_mutex* mutex, uint64_t pkey, uint64_t pvalue);
    bool Delete(std::shared_mutex* mutex, uint64_t pkey);
  };

  pmem::obj::pool_base pop_;

  struct Root {
    pmem::obj::persistent_ptr<Entry[]> entry;
    uint64_t nr_entry;
    std::atomic<uint64_t> size;
  };

  pmem::obj::persistent_ptr<Root> root_;
  Entry** in_mem_entry_;
  uint64_t* in_mem_key_;
  std::shared_mutex* locks_;

  Entry* GetEntry_(int index) const {
    return in_mem_entry_[index];
  }

  uint64_t Find_(uint64_t key, uint64_t begin, uint64_t end) const;
};

class BLevel::Iter : public Iterator {
 public:
  explicit Iter(BLevel* blevel) : blevel_(blevel), entry_index_(0) {}
  ~Iter() {};

  bool Valid() const {
    if (entry_index_ < 0 || entry_index_ >= blevel_->EntrySize())
      return false;
    if (EntryType_() == BLevel::Entry::Type::ENTRY_NONE)
      return false;
    else if (EntryType_() == BLevel::Entry::Type::ENTRY_CLVEL)
      return clevel_iter_ == nullptr || clevel_iter_->Valid();
    else if (EntryType_() == BLevel::Entry::Type::ENTRY_VALUE)
      return true;
  }

  bool Begin() const {
    if (entry_index_ == 0) {
      if (entry_type_ == BLevel::Entry::Type::ENTRY_CLVEL)
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
      if (EntryType_() == BLevel::Entry::Type::ENTRY_CLVEL)
        return clevel_iter_ && clevel_iter_->End();
    }
    return false;
  }

  void SeekToFirst() {
    entry_index_ = 0;
    Lock_();
    entry_type_ = blevel_->GetEntry_(entry_index_)->type;
    if (entry_type_ == BLevel::Entry::Type::ENTRY_CLVEL) {
      clevel_iter_ = blevel_->GetEntry_(entry_index_)->clevel->begin();
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
    while (blevel_->GetEntry_(entry_index_)->type ==
           BLevel::Entry::Type::ENTRY_NONE) {
      Unlock_();
      entry_index_--;
      Lock_();
    }
    entry_type_ = blevel_->GetEntry_(entry_index_)->type;
    if (entry_type_ == BLevel::Entry::Type::ENTRY_CLVEL) {
      clevel_iter_ = blevel_->GetEntry_(entry_index_)->clevel->end();
    }
  }

  void Seek(uint64_t target) {
  }

  void Next() {
    if (entry_type_ == BLevel::Entry::Type::ENTRY_CLVEL) {
      clevel_iter_->Next();
      if (!clevel_iter_->End())
        return;
    }
    NextEntry_();
  }

  void Prev() { assert(0); }

  uint64_t key() const {
    switch (entry_type_) {
      case BLevel::Entry::Type::ENTRY_CLVEL:
        return clevel_iter_->key();
      case BLevel::Entry::Type::ENTRY_NONE:
        assert(0);
        return 0;
      case BLevel::Entry::Type::ENTRY_VALUE:
        return blevel_->GetEntry_(entry_index_)->key;
      default:
        assert(0);
        break;
    }
  }

  uint64_t value() const {
    switch (entry_type_) {
      case BLevel::Entry::Type::ENTRY_CLVEL:
        return clevel_iter_->value();
      case BLevel::Entry::Type::ENTRY_NONE:
        assert(0);
        return 0;
      case BLevel::Entry::Type::ENTRY_VALUE:
        return blevel_->GetEntry_(entry_index_)->value;
      default:
        assert(0);
        break;
    }
  }

 private:
  BLevel* blevel_;
  uint64_t entry_index_;
  uint64_t begin_entry_index_;
  Iterator* clevel_iter_;
  BLevel::Entry::Type entry_type_;

  void Lock_() {
    blevel_->locks_[entry_index_].lock();
  }

  void Unlock_() {
    blevel_->locks_[entry_index_].unlock();
  }

  BLevel::Entry::Type EntryType_() const {
    return blevel_->GetEntry_(entry_index_)->type;
  }

  void NextEntry_() {
    Unlock_();
    if (clevel_iter_)
      delete clevel_iter_;
    clevel_iter_ = nullptr;
    entry_index_++;
    while (!End() && !Valid())
      entry_index_++;
    if (End())
      return;
    Lock_();
    entry_type_ = blevel_->GetEntry_(entry_index_)->type;
    if (entry_type_ == BLevel::Entry::Type::ENTRY_CLVEL) {
      clevel_iter_ = blevel_->GetEntry_(entry_index_)->clevel->begin();
    }
  }

};

} // namespace combotree