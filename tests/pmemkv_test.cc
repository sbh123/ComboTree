#include <cassert>
#include "pmemkv.h"

using namespace combotree;

#define PMEMKV_PATH "/mnt/pmem0/pmemkv"

int main(void) {
  PmemKV* db;
  db = new PmemKV(PMEMKV_PATH);

  for (int i = 0; i < 100; ++i) {
    db->Insert(i, i);
  }

  char buf[8];
  *(uint64_t*)buf = 10;
  pmem::kv::string_view str(buf, 8);

  for (int i = 0; i < 100; ++i) {
    uint64_t value;
    bool res = db->Get(i, value);
    assert(res && value == (uint64_t)i);
  }

  for (int i = 100; i < 200; ++i) {
    uint64_t value;
    bool res = db->Get(i, value);
    assert(!res);
  }

  for (int i = 0; i < 100; ++i) {
    bool res = db->Delete(i);
    assert(res);
  }

  return 0;
}