#include "pgm_index.h"

#include <iostream>
#include "combotree_config.h"
#include "alevel.h"

namespace combotree {

int PGM_Index::file_id_ = 0;

PGM_Index::PGM_Index(BLevel* blevel, int span)
    : span_(span), blevel_(blevel)
{
  // actual blevel entry count is blevel_->nr_entry_ - 1
  // because the first entry in blevel is 0
  nr_blevel_entry_ = blevel_->Entries() - 1;
  min_key_ = blevel_->EntryKey(1);
  max_key_ = blevel_->EntryKey(nr_blevel_entry_);
  nr_entry_ = ((nr_blevel_entry_ + 1) / span_) + 1;

  size_t file_size = nr_entry_ * sizeof(uint64_t);
  pmem_file_ = std::string(PGM_INDEX_PMEM_FILE) + std::to_string(file_id_++);
  int is_pmem;
  std::filesystem::remove(pmem_file_);
  pmem_addr_ = pmem_map_file(pmem_file_.c_str(), file_size + 64,
               PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666, &mapped_len_, &is_pmem);
  // assert(is_pmem == 1);
  if (pmem_addr_ == nullptr) {
    perror("BLevel::BLevel(): pmem_map_file");
    exit(1);
  }
  // aligned at 64-bytes
  key_index = (uint64_t*)pmem_addr_;
  if (((uintptr_t)key_index & (uintptr_t)63) != 0) {
    key_index = (uint64_t*)(((uintptr_t)key_index+64) & ~(uintptr_t)63);
  }

  key_index[0] = min_key_;
  uint64_t offset = 0;
  int index = 0;
  while(offset <  blevel_->Entries()) {
      key_index[index ++] = blevel_->EntryKey(offset + 1);
      offset += span;
  }
  key_index[nr_entry_ - 1] = max_key_;
  pgm_index = pgm::PGMIndex<uint64_t>(key_index, key_index + nr_entry_);
  LOG(Debug::INFO, "PGM-Index segments is %ld.", pgm_index.segments_count());
  {
    //store segments and levelsize and levelcount
  }
}

PGM_Index::~PGM_Index() {
  pmem_unmap(pmem_addr_, mapped_len_);
  std::filesystem::remove(pmem_file_);
}

void PGM_Index::GetBLevelRange_(uint64_t key, uint64_t& begin, uint64_t& end) const {
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

  auto range = pgm_index.search(key);

  end = std::lower_bound(key_index + range.lo,  key_index + range.hi, key) - key_index;
  end *= span_;
  begin = end - span_;
}

} // namespace combotree