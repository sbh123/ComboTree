#include <iostream>
#include "alevel.h"

namespace combotree {

ALevel::ALevel(BLevel* blevel, int span)
    : span_(span), blevel_(blevel)
{
  min_key_ = blevel_->MinEntryKey();
  max_key_ = blevel_->MaxEntryKey();
  // actual blevel entry count is blevel_->nr_entry_ - 1
  // because the first entry in blevel is 0
  nr_blevel_entry_ = blevel_->EntrySize() - 1;
  nr_entry_ = CDFIndex_(max_key_) + 1;
  entry_ = new Entry[nr_entry_];

  entry_[0].key = min_key_;
  entry_[0].offset = 1;
  for (uint64_t offset = 2; offset < blevel_->EntrySize(); ++offset) {
    // calculate cdf and index for every key
    uint64_t cur_key = blevel_->GetEntry_(offset)->key;
    int index = CDFIndex_(cur_key);
    if (entry_[index].key == 0) {
      entry_[index].key = cur_key;
      entry_[index].offset = offset;
      for (int i = index - 1; i > 0; --i) {
        if (entry_[i].key != 0) break;
        entry_[i].key = cur_key;
        entry_[i].offset = offset;
      }
    }
  }
  entry_[nr_entry_ - 1].key = max_key_;
  entry_[nr_entry_ - 1].offset = nr_blevel_entry_;
}

void ALevel::GetBLevelRange_(uint64_t key, uint64_t& begin, uint64_t& end) const {
  if (key < min_key_) {
    begin = 0;
    end = 0;
    return;
  }
  if (key >= max_key_) {
    begin = entry_[nr_entry_ - 1].offset;
    end = entry_[nr_entry_ - 1].offset;
    return;
  }

  uint64_t cdf_index = CDFIndex_(key);
  if (key >= entry_[cdf_index].key) {
    begin = entry_[cdf_index].offset;
    if (cdf_index == nr_entry_ - 1)
      end = begin;
    else
      end = entry_[cdf_index + 1].offset;
  } else {
    begin = entry_[cdf_index - 1].offset;
    end = entry_[cdf_index].offset;
    // assert(begin != end);
    if (begin == end) begin--;
  }
}

} // namespace combotree