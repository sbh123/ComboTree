#include <iostream>
#include <filesystem>
#include "combotree_config.h"
#include "rmi_index.h"
#include "alevel.h"

namespace combotree {

int RMI_Index::file_id_ = 0;

RMI_Index::RMI_Index(BLevel* blevel, int span)
    : span_(span), blevel_(blevel)
{
  // actual blevel entry count is blevel_->nr_entry_ - 1
  // because the first entry in blevel is 0
  nr_blevel_entry_ = blevel_->Entries() - 1;
  min_key_ = blevel_->EntryKey(1);
  max_key_ = blevel_->EntryKey(nr_blevel_entry_);
  nr_entry_ = nr_blevel_entry_ + 1;

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
  uint64_t offset = 0;
  std::vector<rmi_key_t> trains_key;
  while(offset <  blevel_->Entries()) {
      trains_key.push_back(rmi_key_t(blevel_->EntryKey(offset + 1)));
      offset ++;
      // std::cout << "Key index " << index << " : " << key_index[index-1] << std::endl;
  }
  rmi_index.init(trains_key);
  LOG(Debug::INFO, "RMI-Index two stage line model counts is %ld.", 
        rmi_index.rmi_model_n());
  {
    //store segments and levelsize and levelcount
  }
}

RMI_Index::~RMI_Index() {
  pmem_unmap(pmem_addr_, mapped_len_);
  std::filesystem::remove(pmem_file_);
}

void RMI_Index::GetBLevelRange_(uint64_t key, uint64_t& begin, uint64_t& end) {
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
  rmi_key_t rmi_key(key);
  size_t predict_pos = rmi_index.predict(rmi_key);
  int step = 1;
  if(key < blevel_->EntryKey(predict_pos)) {
    end = predict_pos;
    begin = end - step;
    while ((int)begin >= 0 && blevel_->EntryKey(begin) >= key) {
      step = step * 2;
      end = begin;
      begin = end - step;
    }  
    if (((int)begin) < 0) {
      begin = 0;
    }
  } else {
    begin = predict_pos;
    end = begin + step;
    while (end <= nr_blevel_entry_ && blevel_->EntryKey(end) < key) {
      step = step * 2;
      begin = end;
      end = begin + step;
    }  
    if (end > nr_blevel_entry_) {
      end = nr_blevel_entry_;
    }
  }
//   LOG(Debug::INFO, "RMI-Index Get key %lx at range (%lx : %lx) (%ld : %ld)", 
//         key, blevel_->EntryKey(begin), blevel_->EntryKey(end), begin, end);
  // end = std::lower_bound(key_index + range.lo,  key_index + range.hi, key) - key_index;
}

size_t RMI_Index::GetNearPos_(uint64_t key) {
  if (key < min_key_) {
    return 0;
  }
  if (key >= max_key_) {
    return nr_blevel_entry_;
  }
  rmi_key_t rmi_key(key);
  return rmi_index.predict(rmi_key);
}

} // namespace combotree