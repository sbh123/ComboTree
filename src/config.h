#pragma once

#include <cstdint>
#include <string>
#include "slab.h"
#include "clevel.h"

namespace combotree {

class Config {
 public:

  static inline void* __attribute__((always_inline)) PmemAddr(uint64_t offset) {
    return (void*)(offset + base_addr_);
  }

  static void SetBaseAddr(void* addr) { base_addr_ = cur_addr_ = (uint64_t)addr; }
  static void* Allocate(size_t size) {
    void* ret = (void*)cur_addr_;
    cur_addr_ += size;
    return ret;
  }

  static uint64_t GetBaseAddr() { return base_addr_; }

  static CLevel::MemControl* CLevelMem() { return clevel_mem_; }
  static pmem::obj::pool_base* PoolBase() { return &pop_; }
  static Slab<CLevel::LeafNode>* CLevelLeafSlab() { return clevel_leaf_slab_; }
  static Slab<CLevel>* CLevelSlab() { return clevel_slab_; }

  static void SetPmemObjFile(std::string file_name, size_t file_size) {
    pmemobj_file_ = file_name;
    pop_ = pmem::obj::pool_base::create(pmemobj_file_, "Combo Tree",
                                        file_size, 0666);
    clevel_leaf_slab_ = new Slab<CLevel::LeafNode>(pop_, 256);
    clevel_slab_ = new Slab<CLevel>(pop_, 256);
    clevel_mem_ = new CLevel::MemControl(pop_, clevel_leaf_slab_);
  }

 private:
  static uint64_t base_addr_;
  static uint64_t cur_addr_;

  static CLevel::MemControl* clevel_mem_;
  static Slab<CLevel::LeafNode>* clevel_leaf_slab_;
  static Slab<CLevel>* clevel_slab_;
  static pmem::obj::pool_base pop_;
  static std::string pmem_file_;
  static std::string pmemobj_file_;

#ifdef OUTLINE_VALUE
  static uint64_t value_base_addr_;
  std::string value_pmem_file_;
#endif

};

}