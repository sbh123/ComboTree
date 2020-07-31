#include <unistd.h>
#include "clevel.h"

using combotree::CLevel;

#define PATH  "/mnt/pmem0/clevel_test"

int main(void) {
  system("rm -f " PATH);
  auto pop = pmem::obj::pool_base::create(PATH, "CLevel Test",
                                          PMEMOBJ_MIN_POOL * 128, 0666);
  CLevel::SetPoolBase(pop);

  CLevel clevel;

  for (uint64_t i = 0; i < 120000; i += 2) {
    bool res = clevel.Insert(i, i);
    assert(res);
  }

  for (uint64_t i = 0; i < 120000; i += 2) {
    uint64_t value;
    bool find = clevel.Get(i, value);
    assert(find && value == i);
  }

  return 0;
}