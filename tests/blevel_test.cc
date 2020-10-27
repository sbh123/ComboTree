#include <iostream>
#include "../src/blevel.h"
#include "../src/config.h"
#include "test_base.h"
#include "random.h"
#include <set>
#include "../src/combotree_config.h"

#define TEST_SIZE   100000

namespace combotree {

uint64_t Config::base_addr_;
uint64_t Config::cur_addr_;

CLevel::MemControl* Config::clevel_mem_;
Slab<CLevel::LeafNode>* Config::clevel_leaf_slab_;
Slab<CLevel>* Config::clevel_slab_;
pmem::obj::pool_base Config::pop_;
std::string Config::pmem_file_;
std::string Config::pmemobj_file_;

std::mutex log_mutex;

class Test : TestBase {
 public:
  Test(std::string pmem_file, size_t pmem_size, std::string pmemobj_file, size_t pmemobj_size)
    :TestBase(pmem_file, pmem_size, pmemobj_file, pmemobj_size) {}

  int Run() {
    std::set<uint64_t> key_set;
    Random rnd(0, 100000000);

    for (int i = 0; i < TEST_SIZE; ++i)
      key_set.emplace(rnd.Next());

    std::vector<uint64_t> key(key_set.begin(), key_set.end());

    BLevel::Entry* ent;
    ent = new (addr) BLevel::Entry(0,0,2);
    ent->Delete(0, nullptr);

    uint64_t value;

    // Put
    for (int i = 0; i < key.size(); ++i)
      assert(ent->Put(key[i], key[i]) == true);

    // Get
    for (int i = 0; i < key.size(); ++i) {
      assert(ent->Get(key[i], value) == true);
      assert(value == key[i]);
    }

    // Delete
    for (int i = 0; i < key.size(); ++i) {
      assert(ent->Delete(key[i], &value) == true);
    }

    for (int i = 0; i < key.size(); ++i) {
      assert(ent->Get(key[i], value) == false);
    }

    return 0;
  }
};

}

int main() {
  combotree::Test test(TEST_PMEM_FILE, 1024*1024*1024,
                       TEST_PMEMOBJ_FILE, 1024*1024*512);
  return test.Run();
}
