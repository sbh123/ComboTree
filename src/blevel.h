#pragma once

#include <cstdint>
#include <cassert>
#include <libpmemobj++/persistent_ptr.hpp>
#include "iterator.h"
#include "clevel.h"

namespace combotree {

class ALevel;

class BLevel {
 public:
  BLevel(pmem::obj::pool_base& pop, Iterator* iter, uint64_t size);
  BLevel(pmem::obj::pool_base& pop);

  bool Get(uint64_t key, uint64_t& value, uint64_t begin = 0, uint64_t end = UINT64_MAX) const {
    if (end == UINT64_MAX) end = EntrySize() - 1;
    uint64_t idx = Find_(key, begin, end);
    return GetEntry_(idx)->Get(key, value);
  }

  bool Insert(uint64_t key, uint64_t value, uint64_t begin = 0, uint64_t end = UINT64_MAX) {
    if (end == UINT64_MAX) end = EntrySize() - 1;
    uint64_t idx = Find_(key, begin, end);
    bool res = GetEntry_(idx)->Insert(pop_, key, value);
    if (res) {
      root_->size_++;
    }
    return res;
  }

  bool Update(uint64_t key, uint64_t value, uint64_t begin = 0, uint64_t end = UINT64_MAX) {
    if (end == UINT64_MAX) end = EntrySize() - 1;
    uint64_t idx = Find_(key, begin, end);
    return GetEntry_(idx)->Update(key, value);
  }

  bool Delete(uint64_t key, uint64_t begin = 0, uint64_t end = UINT64_MAX) {
    if (end == UINT64_MAX) end = EntrySize() - 1;
    uint64_t idx = Find_(key, begin, end);
    bool res = GetEntry_(idx)->Delete(key);
    if (res) {
      root_->size_--;
    }
    return res;
  }

  uint64_t MinKey() const;
  uint64_t MaxKey() const;

  uint64_t MinEntryKey() const;
  uint64_t MaxEntryKey() const;

  uint64_t Size() const { return root_->size_; }
  uint64_t EntrySize() const { return root_->nr_entry_; }

  Iterator* begin();
  Iterator* end();

  class Iter;

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
    bool Get(uint64_t pkey, uint64_t& pvalue) const;
    bool Insert(pmem::obj::pool_base& pop, uint64_t pkey, uint64_t pvalue);
    bool Update(uint64_t pkey, uint64_t pvalue);
    bool Delete(uint64_t pkey);
  };

  pmem::obj::pool_base& pop_;

  struct Root {
    pmem::obj::persistent_ptr<Entry[]> entry_;
    uint64_t nr_entry_;
    uint64_t size_;
  };

  pmem::obj::persistent_ptr<Root> root_;

  Entry* GetEntry_(int index) const {
    return &root_->entry_[index];
  }

  uint64_t Find_(uint64_t key, uint64_t begin, uint64_t end) const;
};

class BLevel::Iter : public Iterator {
 public:
  explicit Iter(BLevel* blevel) : blevle_(blevel), entry_index_(0) {}
  ~Iter() {};

  bool Begin() const {
    if (entry_index_ > begin_entry_index_) {
      return false;
    } else if (entry_index_ == begin_entry_index_) {
      if (entry_type_ == BLevel::Entry::Type::ENTRY_CLVEL)
        return clevel_iter_->Begin();
      else
        return true;
    } else {
      assert(0);
      return true;
    }
  }

  bool End() const {
    if (entry_index_ < blevle_->EntrySize() - 1) {
      return false;
    } else {
      if (entry_type_ == BLevel::Entry::Type::ENTRY_CLVEL)
        return clevel_iter_->End();
      else
        return true;
    }
  }

  void SeekToFirst() {
    entry_index_ = 0;
    while (blevle_->GetEntry_(entry_index_)->type ==
           BLevel::Entry::Type::ENTRY_NONE) {
      entry_index_++;
    }
    entry_type_ = blevle_->GetEntry_(entry_index_)->type;
    if (entry_type_ == BLevel::Entry::Type::ENTRY_CLVEL) {
      clevel_iter_ = blevle_->GetEntry_(entry_index_)->clevel->begin();
    }
  }

  void SeekToLast() {
    entry_index_ = blevle_->EntrySize() - 1;
    while (blevle_->GetEntry_(entry_index_)->type ==
           BLevel::Entry::Type::ENTRY_NONE) {
      entry_index_--;
    }
    entry_type_ = blevle_->GetEntry_(entry_index_)->type;
    if (entry_type_ == BLevel::Entry::Type::ENTRY_CLVEL) {
      clevel_iter_ = blevle_->GetEntry_(entry_index_)->clevel->end();
    }
  }

  void Seek(uint64_t target) {
  }

  void Next() {
    if (entry_type_ == BLevel::Entry::Type::ENTRY_CLVEL) {
      if (!clevel_iter_->End()) {
        clevel_iter_->Next();
        return;
      }
    }
    entry_index_++;
    while (blevle_->GetEntry_(entry_index_)->type ==
           BLevel::Entry::Type::ENTRY_NONE) {
      entry_index_++;
    }
    entry_type_ = blevle_->GetEntry_(entry_index_)->type;
    if (entry_type_ == BLevel::Entry::Type::ENTRY_CLVEL) {
      clevel_iter_ = blevle_->GetEntry_(entry_index_)->clevel->begin();
    }
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
        return blevle_->GetEntry_(entry_index_)->key;
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
        return blevle_->GetEntry_(entry_index_)->value;
      default:
        assert(0);
        break;
    }
  }

 private:
  BLevel* blevle_;
  uint64_t entry_index_;
  uint64_t begin_entry_index_;
  Iterator* clevel_iter_;
  BLevel::Entry::Type entry_type_;
};

} // namespace combotree