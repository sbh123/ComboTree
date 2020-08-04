#include <iostream>
#include <map>
#include <filesystem>
#include <fstream>
#include "blevel.h"
#include "iterator.h"
#include "std_map_iterator.h"
#include "random.h"

using namespace std;

#define TEST_SIZE 4000000

#define POOL_PATH   "/mnt/pmem0/persistent"
#define POOL_LAYOUT "Combo Tree"
#define POOL_SIZE   (PMEMOBJ_MIN_POOL * 400)

int main(void) {
  std::filesystem::remove(POOL_PATH);
  auto pop = pmem::obj::pool_base::create(POOL_PATH, POOL_LAYOUT, POOL_SIZE, 0666);

  fstream f("/home/qyzhang/Projects/ComboTree/build/workload.txt", ios::in);

  std::map<uint64_t, uint64_t> kv;
  std::map<uint64_t, uint64_t> right_kv;
  combotree::RandomUniformUint64 rnd;
  uint64_t key, value;

  for (int i = 0; i < 1024; ++i) {
    f >> key;
    kv.emplace(key, key);
    right_kv.emplace(key, key);
  }

  combotree::Iterator* iter = new combotree::MapIterator(&kv);
  iter->SeekToFirst();
  combotree::BLevel* db = new combotree::BLevel(pop, iter, kv.size());

  // for (int i = 100; i < TEST_SIZE; ++i) {
  //   f >> key;
  //   bool res = db->Insert(key, key);
  //   assert(res);
  // }

  // f.seekg(0);
  // for (int i = 0; i < TEST_SIZE; ++i) {
  //   f >> key;
  //   bool res = db->Get(key, value);
  //   assert(res && value == key);
  // }

  // for (int i = 0; i < 70000 - TEST_SIZE; ++i) {
  //   uint64_t key;
  //   f >> key;
  //   uint64_t value;
  //   bool res = db->Get(key, value);
  //   assert(!res);
  // }

  for (int i = 0; i < TEST_SIZE; ++i) {
    int op = rnd.Next();
    uint64_t key = rnd.Next();
    uint64_t value;
    uint64_t right_value;
    bool res;
    switch (op % 3) {
      case 0: // PUT
        value = rnd.Next();
        if (right_kv.count(key)) {
          res = db->Insert(key, value);
          assert(!res);
        } else {
          right_kv.emplace(key, value);
          res = db->Insert(key, value);
          assert(res);
        }
        break;
      case 1: // GET
        if (right_kv.count(key)) {
          right_value = right_kv.at(key);
          res = db->Get(key, value);
          assert(res && right_value == value);
        } else {
          res = db->Get(key, value);
          assert(!res);
        }
        break;
      case 2: // DELETE
        if (right_kv.count(key)) {
          right_kv.erase(key);
          res = db->Delete(key);
          assert(res);
        } else {
          res = db->Delete(key);
          assert(!res);
        }
        break;
      // case 3: // UPDATE
      //   break;
    }
    assert(db->Size() == right_kv.size());
  }

  pop.close();

  return 0;
}