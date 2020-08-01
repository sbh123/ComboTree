#include <iostream>
#include <map>
#include <filesystem>
#include "blevel.h"
#include "iterator.h"
#include "std_map_iterator.h"

using namespace combotree;

#define POOL_PATH   "/mnt/pmem0/persistent"
#define POOL_LAYOUT "Combo Tree"
#define POOL_SIZE   (PMEMOBJ_MIN_POOL * 400)

int main(void) {
  std::filesystem::remove(POOL_PATH);
  auto pop = pmem::obj::pool_base::create(POOL_PATH, POOL_LAYOUT, POOL_SIZE, 0666);

  std::map<uint64_t, uint64_t> kv;
  for (int i = 0; i < 2000; i += 2) {
    kv.emplace(i, i);
  }

  Iterator* iter = new MapIterator(&kv);
  iter->SeekToFirst();
  BLevel* blevel = new BLevel(pop, iter, kv.size());

  for (int i = 1; i < 2000; i += 2) {
    bool res = blevel->Insert(i, i);
    assert(res);
  }

  for (int i = 0; i < 2000; ++i) {
    bool find;
    uint64_t value;
    find = blevel->Get(i, value);
    assert(find && value == (uint64_t)i);
  }

  pop.close();

  return 0;
}