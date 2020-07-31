#include <unistd.h>
#include "iterator.h"
#include "clevel.h"

using combotree::CLevel;
using combotree::Iterator;

#define PATH  "/mnt/pmem0/clevel_test"

int main(void) {
  system("rm -f " PATH);
  auto pop = pmem::obj::pool_base::create(PATH, "CLevel Test",
                                          PMEMOBJ_MIN_POOL * 128, 0666);
  CLevel::SetPoolBase(pop);

  CLevel clevel;

  for (uint64_t i = 0; i < 120000; ++i) {
    bool res = clevel.Insert(i, i);
    assert(res);
  }

  for (uint64_t i = 0; i < 1200; ++i) {
    bool res = clevel.Delete(i);
    assert(res);
  }

  for (uint64_t i = 0; i < 1200; ++i) {
    uint64_t value;
    bool find = clevel.Get(i, value);
    assert(!find);
  }

  for (uint64_t i = 1200; i < 120000; ++i) {
    uint64_t value;
    bool find = clevel.Get(i, value);
    assert(find && value == i);
  }

  uint64_t i = 1200;
  for (Iterator* iter = clevel.begin(); !iter->End(); iter->Next()) {
    uint64_t key = iter->key();
    uint64_t value = iter->value();
    assert(key == i && value == i);
    i++;
  }
  assert(i == 120000);

  Iterator* iter = clevel.end();
  do {
    i--;
    uint64_t key = iter->key();
    uint64_t value = iter->value();
    assert(key == i && value == i);
    iter->Prev();
  } while (!iter->Begin());
  assert(i == 1200);

  return 0;
}