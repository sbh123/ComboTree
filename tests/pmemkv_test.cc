#include <filesystem>
#include <cassert>
#include "pmemkv.h"

using namespace combotree;

#define PMEMKV_PATH "/mnt/pmem0/persistent"

int main(void) {
  std::filesystem::remove(PMEMKV_PATH);
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
    Status s = db->Get(i, value);
    assert(s == Status::OK && value == (uint64_t)i);
  }

  for (int i = 100; i < 200; ++i) {
    uint64_t value;
    Status s = db->Get(i, value);
    assert(s == Status::DOES_NOT_EXIST);
  }

  for (int i = 0; i < 100; ++i) {
    Status s = db->Delete(i);
    assert(s == Status::OK);
  }

  return 0;
}