#include <iostream>
#include <cassert>
#include <set>
#include "../include/combotree/combotree.h"
#include "random.h"
#include "timer.h"

#define TEST_SIZE   2000000
#define LAST_EXPAND 1520000

using combotree::ComboTree;
using combotree::Random;
using combotree::Timer;

int main(void) {
#ifdef SERVER
  ComboTree* tree = new ComboTree("/mnt/pmem0/combotree/", (1024*1024*1024*100UL), true);
#else
  ComboTree* tree = new ComboTree("/mnt/pmem0/combotree/", (1024*1024*1024*1UL), true);
#endif

  std::vector<uint64_t> key;
  Random rnd(0, TEST_SIZE-1);

  for (int i = 0; i < TEST_SIZE; ++i)
    key.push_back(i);

  for (int i = 0; i < TEST_SIZE; ++i)
    std::swap(key[i],key[rnd.Next()]);

  uint64_t value;
  Timer timer;

  // Put
  timer.Record("start");
  for (size_t i = 0; i < LAST_EXPAND; ++i)
    assert(tree->Put(key[i], key[i]) == true);
  timer.Record("mid");
  for (size_t i = LAST_EXPAND; i < TEST_SIZE; ++i)
    assert(tree->Put(key[i], key[i]) == true);
  timer.Record("stop");

  uint64_t total_time = timer.Microsecond("stop", "start");
  std::cout << "put: " << total_time/1000000.0 << " " << (double)TEST_SIZE/(double)total_time*1000000 << std::endl;
  uint64_t mid_time = timer.Microsecond("stop", "mid");
  std::cout << "put: " << mid_time/1000000.0 << " " << (double)(TEST_SIZE-LAST_EXPAND)/(double)mid_time*1000000 << std::endl;

  std::cout << "clevel time: " << tree->CLevelTime()/1000000.0 << std::endl;

  timer.Clear();

  std::cout << "entries: " << tree->BLevelEntries() << std::endl;
  std::cout << "clevels: " << tree->CLevelCount() << std::endl;
  tree->BLevelCompression();

  // Get
  timer.Record("start");
  for (auto& k : key) {
    assert(tree->Get(k, value) == true);
    assert(value == k);
  }

  timer.Record("stop");
  total_time = timer.Microsecond("stop", "start");
  std::cout << "get: " << total_time/1000000.0 << " " << (double)key.size()/(double)total_time*1000000 << std::endl;

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

  return 0;
}