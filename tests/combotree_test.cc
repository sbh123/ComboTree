#include <iostream>
#include <cassert>
#include <set>
#include "../include/combotree/combotree.h"
#include "random.h"

#define TEST_SIZE   5000000

using combotree::ComboTree;
using combotree::Random;

int main(void) {
  ComboTree* tree = new ComboTree("/mnt/pmem0/combotree/", (1024*1024*1024*1UL), true);

  std::set<uint64_t> key_set;
  Random rnd(0, 100000000);

  for (int i = 0; i < TEST_SIZE; ++i)
    key_set.emplace(rnd.Next());

  std::vector<uint64_t> key(key_set.begin(), key_set.end());

  uint64_t value;

  // Put
  for (auto& k : key)
    assert(tree->Put(k, k) == true);

  // Get
  for (auto& k : key) {
    assert(tree->Get(k, value) == true);
    assert(value == k);
  }
  for (int i = 0; i < 10000000; ++i) {
    if (key_set.count(i) == 0)
      assert(tree->Get(i, value) == false);
  }

  // Delete
  for (auto& k : key) {
    assert(tree->Delete(k) == true);
  }
  for (auto& k : key) {
    assert(tree->Get(k, value) == false);
  }

  std::cout << "entries: " << tree->BLevelEntries() << std::endl;
  std::cout << "clevels: " << tree->CLevelCount() << std::endl;

  return 0;
}