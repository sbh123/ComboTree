#include <cassert>
#include <string>
#include <libpmem.h>
#include "../src/config.h"

namespace combotree {

class TestBase {
 public:
  TestBase(std::string pmem_file, size_t pmem_size,
           std::string pmemobj_file, size_t pmemobj_size) {
    system((std::string("rm -rf ")+pmem_file).c_str());
    system((std::string("rm -rf ")+pmemobj_file).c_str());

    int is_pmem;
    pmemaddr_ = pmem_map_file(pmem_file.c_str(), pmem_size+64,
                PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666, &mapped_len, &is_pmem);
    // assert(is_pmem == 1);
    if (pmemaddr_ == NULL) {
      perror("pmem_map_file");
      exit(1);
    }

    // aligned at 64-bytes
    addr = pmemaddr_;
    if (((uintptr_t)pmemaddr_ & (uintptr_t)63) != 0) {
      // not aligned
      addr = (void*)(((uintptr_t)addr+64) & ~(uintptr_t)63);
    }

    Config::SetBaseAddr(addr);
    Config::SetPmemObjFile(pmemobj_file, pmemobj_size);

    std::cout << "clflush method : " << FLUSH_METHOD << std::endl;
    std::cout << "pmem addr    : " << pmemaddr_ << std::endl;
    std::cout << "aligned addr : " << addr << std::endl;
  }

  ~TestBase() {
    pmem_unmap(pmemaddr_, mapped_len);
  }

  virtual int Run() { return 0; }

 protected:
  void* addr;
  size_t mapped_len;

 private:
  void* pmemaddr_;
};

}