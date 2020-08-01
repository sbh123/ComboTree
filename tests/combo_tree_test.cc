#include <filesystem>
#include "combo-tree/combo_tree.h"

using namespace combotree;

#define COMBO_TREE_DIR  "/mnt/pmem0/combotree/"

int main(void) {
  std::filesystem::remove_all(COMBO_TREE_DIR);
  std::filesystem::create_directory(COMBO_TREE_DIR);
  ComboTree* db;
  db = new ComboTree(COMBO_TREE_DIR, PMEMOBJ_MIN_POOL * 128);

  bool res;
  for (uint64_t i = 1; i < 110000; ++i) {
    res = db->Insert(i, i);
    assert(res);
  }

  for (uint64_t i = 0; i < 110000; ++i) {
    uint64_t value;
    res = db->Get(i, value);
    if (i == 0)
      assert(!res);
    else
      assert(res && value == i);
  }
}