#pragma once

#include <cstdint>
#include <string>

namespace combotree {

// class CLevel;
// class CLevel::MemoryManagement;

class Config {
 public:

  static inline void* __attribute__((always_inline)) PmemAddr(uint64_t offset) {
    return (void*)(offset + base_addr_);
  }

  static void SetBaseAddr(void* addr) { base_addr_ = (uint64_t)addr; }
  static void* Allocate(size_t size);
  static uint64_t GetBaseAddr();

  // static CLevel::MemoryManagement* CLevelMem() { return clevel_mem_; }

 private:
  static uint64_t base_addr_;
  // static CLevel::MemoryManagement* clevel_mem_;
  static
  std::string pmem_file_;
#ifdef OUTLINE_VALUE
  static uint64_t value_base_addr_;
  std::string value_pmem_file_;
#endif

};

}