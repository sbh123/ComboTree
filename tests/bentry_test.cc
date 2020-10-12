#include <iostream>
#include "../src/blevel.h"
#include "../src/config.h"
#include "test_base.h"
#include "../src/combotree_config.h"

namespace combotree {

uint64_t Config::base_addr_;

class Test : TestBase {
 public:
  Test(std::string pmem_file, size_t pmem_size, std::string pmemobj_file, size_t pmemobj_size)
    :TestBase(pmem_file, pmem_size, pmemobj_file, pmemobj_size) {}

  int Run() {
    BLevel::Entry* ent;
    ent = new (addr) BLevel::Entry(28,200,7);
    uint64_t key[] = {
      10,
      39,
      12,
      1,
      0,
      9,
      3,
      40,
    };
    uint64_t value;

    // Put
    for (int i = 0; i < sizeof(key)/sizeof(uint64_t); ++i)
      assert(ent->Put(key[i], key[i]) == true);
    // Get
    for (int i = 0; i < sizeof(key)/sizeof(uint64_t); ++i) {
      assert(ent->Get(key[i], value) == true);
      assert(value == key[i]);
    }

    // Update
    for (int i = 0; i < sizeof(key)/sizeof(uint64_t); ++i)
      assert(ent->Put(key[i], key[i]+1) == true);
    for (int i = 0; i < sizeof(key)/sizeof(uint64_t); ++i) {
      assert(ent->Get(key[i], value) == true);
      assert(value == key[i]+1);
    }
    for (int i = 41; i < 128; ++i)
      assert(ent->Get(i, value) == false);
    assert(ent->Get(28, value) == true);
    assert(value == 200);

    // Delete
    assert(ent->Delete(28, &value) == true);
    assert(value == 200);
    for (int i = 0; i < sizeof(key)/sizeof(uint64_t); ++i) {
      assert(ent->Delete(key[i], &value) == true);
      assert(value == key[i]+1);
    }
    for (int i = 0; i < 128; ++i)
      assert(ent->Delete(i, nullptr) == false);
    return 0;
  }
};

}

int main() {
  combotree::Test test(TEST_PMEM_FILE, 1024*1024*1024*2,
                       TEST_PMEMOBJ_FILE, 1024*1024*512);
  return test.Run();
}
