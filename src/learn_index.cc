#include <iostream>
#include <filesystem>
#include "combotree_config.h"
#include "manifest.h"
#include "learn_index.h"

namespace combotree {

int Learn_Index::file_id_ = 0;

Learn_Index::Learn_Index(BLevel* blevel, int span)
    : span_(span), blevel_(blevel)
{
  Timer timer;
  timer.Start();
  nr_blevel_entry_ = blevel_->Entries() - 1;
  min_key_ = blevel_->EntryKey(1);
  max_key_ = blevel_->EntryKey(nr_blevel_entry_);
  nr_entry_ = nr_blevel_entry_ + 1;

  pmem_file_ = std::string(PGM_INDEX_PMEM_FILE) + "learn" + std::to_string(file_id_++);
  pmem_addr_ = PmemMapFile(pmem_file_, nr_entry_ * sizeof(uint64_t) + 64, &mapped_len_);
  uint64_t *key_index = (uint64_t*)pmem_addr_;
  if (((uintptr_t)key_index & (uintptr_t)63) != 0) {
    key_index = (uint64_t*)(((uintptr_t)key_index+64) & ~(uintptr_t)63);
  }

  key_index[0] = min_key_;
  uint64_t offset = 0;
  int index = 0;
  while(offset <  blevel_->Entries()) {
      key_index[index ++] = blevel_->EntryKey(offset + 1);
      offset ++;
      // std::cout << "Key index " << index << " : " << key_index[index-1] << std::endl;
  }
  key_index[nr_entry_ - 1] = max_key_;

  learn_index_ = new LearnIndex(key_index, key_index + nr_entry_);
  uint64_t train_time = timer.End();
  LOG(Debug::INFO, "Learn-Index segments is %lu, train cost %lf s.", 
      learn_index_->segments_count(), (double)train_time/1000000.0);
  {
    //store segments and levelsize and levelcount
  }
}

Learn_Index::~Learn_Index() {
  pmem_unmap(pmem_addr_, mapped_len_);
  std::filesystem::remove(pmem_file_);
  delete learn_index_;
}

void Learn_Index::GetBLevelRange_(uint64_t key, uint64_t& begin, uint64_t& end, bool debug) const {
  if (key < min_key_) {
    begin = 0;
    end = 0;
    return;
  }
  if (key >= max_key_) {
    begin = nr_blevel_entry_;
    end = nr_blevel_entry_;
    return;
  }
  auto range = learn_index_->search(key, debug);

  // end = std::lower_bound(key_index + range.lo,  key_index + range.hi, key) - key_index;
  begin = range.lo;
  end = range.hi;
}

} // namespace combotree