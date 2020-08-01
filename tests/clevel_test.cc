#include <map>
#include <filesystem>
#include <fstream>
#include "iterator.h"
#include "clevel.h"
#include "random.h"

using combotree::CLevel;
using combotree::Iterator;

using namespace std;

#define PATH        "/mnt/pmem0/persistent"
#define TEST_SIZE   500000

int main(void) {
  std::filesystem::remove(PATH);
  auto pop = pmem::obj::pool_base::create(PATH, "CLevel Test",
                                          PMEMOBJ_MIN_POOL * 128, 0666);
  CLevel::SetPoolBase(pop);

  CLevel* db;
  db = new CLevel();
  db->InitLeaf();

  combotree::RandomUniformUint64 rnd;
  // std::map<uint64_t, uint64_t> right_kv;

  // fstream f("/home/qyzhang/Projects/ComboTree/build/workload.txt", ios::in);

  // for (int i = 0; i < TEST_SIZE; ++i) {
  //   uint64_t key;
  //   f >> key;
  //   uint64_t value = key;
  //   bool res = db->Insert(key, value);
  //   assert(res);
  // }

  // f.seekg(0);
  // for (int i = 0; i < TEST_SIZE; ++i) {
  //   uint64_t key;
  //   f >> key;
  //   uint64_t value;
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
  std::map<uint64_t, uint64_t> right_kv;

  for (int i = 0; i < TEST_SIZE; ++i) {
    int op = rnd.Next();
    uint64_t key = rnd.Next();
    uint64_t value;
    uint64_t right_value;
    bool res;
    switch (op % 4) {
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
        // if (right_kv.count(key)) {
        //   right_kv.erase(key);
        //   res = db->Delete(key);
        //   assert(res);
        // } else {
        //   res = db->Delete(key);
        //   assert(!res);
        // }
        // break;
      case 3: // UPDATE
        break;
    }
  }

  return 0;
}