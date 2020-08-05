#include <map>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>
#include "combotree/iterator.h"
#include "clevel.h"
#include "random.h"

using combotree::CLevel;
using combotree::Iterator;

using namespace std;

#define PATH        "/mnt/pmem0/persistent"
#define TEST_SIZE   3000000

int main(void) {
  std::filesystem::remove(PATH);
  auto pop = pmem::obj::pool_base::create(PATH, "CLevel Test",
                                          PMEMOBJ_MIN_POOL * 128, 0666);
  CLevel::SetPoolBase(pop);

  CLevel* db;
  db = new CLevel();
  db->InitLeaf();

  combotree::RandomUniformUint64 rnd;
  std::map<uint64_t, uint64_t> right_kv;

  fstream f("/home/qyzhang/Projects/ComboTree/build/workload.txt", ios::in);
  fstream f_op("/home/qyzhang/Projects/ComboTree/build/workload1.txt", ios::in);

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
  //   // assert(res && value == key);
  // }

  // for (int i = 0; i < 70000 - TEST_SIZE; ++i) {
  //   uint64_t key;
  //   f >> key;
  //   uint64_t value;
  //   bool res = db->Get(key, value);
  //   // assert(!res);
  // }

  // vector<uint64_t> keys;
  // for (int i = 0; i < LEAF_ENTRYS; ++i) {
  //   uint64_t key = rnd.Next();
  //   bool res = db->Insert(key, key);
  //   if (!res) {
  //     cout << "!" << endl;
  //     i--;
  //   } else {
  //     keys.emplace_back(key);
  //   }
  // }

  // for (int i = 10; i < LEAF_ENTRYS; ++i) {
  //   bool res = db->Delete(keys[i]);
  //   assert(res);
  // }
  // keys.resize(10);

  // for (int i = 10; i < LEAF_ENTRYS; ++i) {
  //   uint64_t key = rnd.Next();
  //   bool res = db->Insert(key, key);
  //   if (!res) {
  //     cout << "!" << endl;
  //     i--;
  //   } else {
  //     keys.emplace_back(key);
  //   }
  // }

  // for (int i = 0; i < LEAF_ENTRYS; ++i) {
  //   uint64_t value;
  //   bool res = db->Get(keys[i], value);
  //   assert(res && value == keys[i]);
  // }

  for (int i = 0; i < TEST_SIZE; ++i) {
    uint64_t key;
    int op;
    // f >> key;
    // f_op >> op;
    key = rnd.Next();
    op = rnd.Next();
    uint64_t value;
    uint64_t right_value;
    bool res;
    if (op % 100 == 0) {
      // SCAN
      auto right_iter = right_kv.lower_bound(key);
      auto iter = db->begin();
      iter->Seek(key);
      while (right_iter != right_kv.end()) {
        assert(right_iter->first == iter->key());
        assert(right_iter->second == iter->value());
        right_iter++;
        iter->Next();
      }
      assert(iter->End());
    }
    switch (op % 3) {
      case 0: // PUT
        value = key;
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
      case 3: // UPDATE
        break;
    }
  }

  return 0;
}