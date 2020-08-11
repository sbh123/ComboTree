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
using combotree::Status;

using namespace std;

#define PATH        "/mnt/pmem0/persistent"
#define TEST_SIZE   3000000

int main(void) {
  std::filesystem::remove(PATH);
  auto pop = pmem::obj::pool_base::create(PATH, "CLevel Test",
                                          PMEMOBJ_MIN_POOL * 128, 0666);
  CLevel* db;
  db = new CLevel();
  db->InitLeaf(pop);

  combotree::RandomUniformUint64 rnd;
  std::map<uint64_t, uint64_t> right_kv;

  fstream f("/home/qyzhang/Projects/ComboTree/build/workload.txt", ios::in);
  fstream f_op("/home/qyzhang/Projects/ComboTree/build/workload1.txt", ios::in);

  for (int i = 0; i < TEST_SIZE; ++i) {
    uint64_t key;
    int op;
    // f >> key;
    // f_op >> op;
    key = rnd.Next();
    op = rnd.Next();
    uint64_t value;
    uint64_t right_value;
    Status s;
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
      delete iter;
    }
    switch (op % 3) {
      case 0: // PUT
        value = key;
        if (right_kv.count(key)) {
          s = db->Insert(pop, key, value);
          assert(s == Status::ALREADY_EXISTS);
        } else {
          right_kv.emplace(key, value);
          s = db->Insert(pop, key, value);
          assert(s == Status::OK);
        }
        break;
      case 1: // GET
        if (right_kv.count(key)) {
          right_value = right_kv.at(key);
          s = db->Get(key, value);
          assert(s == Status::OK && right_value == value);
        } else {
          s = db->Get(key, value);
          assert(s == Status::DOES_NOT_EXIST);
        }
        break;
      case 2: // DELETE
        if (right_kv.count(key)) {
          right_kv.erase(key);
          s = db->Delete(pop, key);
          assert(s == Status::OK);
        } else {
          s = db->Delete(pop, key);
          assert(s == Status::DOES_NOT_EXIST);
        }
        break;
    }
  }

  return 0;
}