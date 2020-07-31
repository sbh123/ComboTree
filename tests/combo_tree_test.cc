#include "combo-tree/combo_tree.h"

using namespace combotree;

int main(void) {
  ComboTree* db;
  db = new ComboTree("/mnt/pmem0/", PMEMOBJ_MIN_POOL * 128);

  bool res;
  for (uint64_t i = 0; i < 1100; ++i) {
    res = db->Insert(i, i);
    assert(res);
  }

  for (uint64_t i = 0; i < 1100; ++i) {
    uint64_t value;
    res = db->Get(i, value);
    assert(res && value == i);
  }
}