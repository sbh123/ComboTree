#pragma once

#include <cstdint>
#include <cassert>
#include "combotree/iterator.h"
#include "blevel.h"

namespace combotree {

#define DEFAULT_COMBO_TREE_SPAN 2

class ALevel {
 public:
  ALevel(BLevel* blevel, int span = DEFAULT_COMBO_TREE_SPAN);

  bool Insert(uint64_t key, uint64_t value) {
    uint64_t begin, end;
    GetBLevelRange_(key, begin, end);
    return blevel_->Insert(key, value, begin, end);
  }

  bool Update(uint64_t key, uint64_t value) {
    uint64_t begin, end;
    GetBLevelRange_(key, begin, end);
    return blevel_->Update(key, value, begin, end);
  }

  bool Get(uint64_t key, uint64_t& value) const {
    uint64_t begin, end;
    GetBLevelRange_(key, begin, end);
    return blevel_->Get(key, value, begin, end);
  }

  bool Delete(uint64_t key) {
    uint64_t begin, end;
    GetBLevelRange_(key, begin, end);
    return blevel_->Delete(key, begin, end);
  }

  size_t Size() const {
    return blevel_->Size();
  }

 private:
  struct Entry {
    Entry() : key(0), offset(0) {}

    uint64_t key;
    uint64_t offset;
  };

  int span_;
  BLevel* blevel_;
  uint64_t min_key_;
  uint64_t max_key_;
  uint64_t nr_blevel_entry_;
  uint64_t nr_entry_;
  Entry* entry_;

  double CalculateCDF_(uint64_t key) const {
    return (double)(key - min_key_) / (double)(max_key_ - min_key_);
  }

  uint64_t CDFIndex_(uint64_t key) const {
    if (key < min_key_) return 0;
    if (key > max_key_) return nr_entry_ - 1;
    return (uint64_t)((CalculateCDF_(key) * nr_blevel_entry_ + 1.0) / span_);
  }

  void GetBLevelRange_(uint64_t key, uint64_t& begin, uint64_t& end) const;
};

} // namespace combotree