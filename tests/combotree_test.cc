#include <iostream>
#include <cassert>
#include <set>
#include "../include/combotree/combotree.h"
#include "random.h"

#define TEST_SIZE   10000000

using combotree::ComboTree;
using combotree::Random;

int main(void) {
  ComboTree* tree = new ComboTree("/mnt/pmem0/combotree/", (1024*1024*1024*1UL), true);

  std::vector<uint64_t> key;
  Random rnd(0, TEST_SIZE-1);

  for (int i = 0; i < TEST_SIZE; ++i)
    key.push_back(i);

  for (int i = 0; i < TEST_SIZE/2; ++i)
    std::swap(key[i],key[rnd.Next()]);

  uint64_t value;

  // Put
  for (auto& k : key)
    assert(tree->Put(k, k) == true);

  // Get
  for (auto& k : key) {
    assert(tree->Get(k, value) == true);
    assert(value == k);
  }
  for (uint64_t i = TEST_SIZE; i < TEST_SIZE+100000; ++i) {
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