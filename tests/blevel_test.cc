#include <iostream>
#include <map>
#include <filesystem>
#include <fstream>
#include "blevel.h"
#include "combotree/iterator.h"
#include "std_map_iterator.h"
#include "random.h"

using namespace std;
using combotree::Status;

#define TEST_SIZE 4000000

#define POOL_PATH   "/mnt/pmem0/persistent"
#define POOL_LAYOUT "Combo Tree"
#define POOL_SIZE   (PMEMOBJ_MIN_POOL * 400)

int main(void) {
  std::filesystem::remove(POOL_PATH);
  auto pop = pmem::obj::pool_base::create(POOL_PATH, POOL_LAYOUT, POOL_SIZE, 0666);

  fstream f("/home/qyzhang/Projects/ComboTree/build/workload.txt", ios::in);
  fstream f_op("/home/qyzhang/Projects/ComboTree/build/workload1.txt", ios::in);

  std::map<uint64_t, uint64_t> kv;
  std::map<uint64_t, uint64_t> right_kv;
  combotree::RandomUniformUint64 rnd;

  for (int i = 0; i < 1024; ++i) {
    uint64_t key;
    f >> key;
    kv.emplace(key, key);
    right_kv.emplace(key, key);
  }

  combotree::Iterator* iter = new combotree::MapIterator(&kv);
  iter->SeekToFirst();
  combotree::BLevel* db = new combotree::BLevel(pop, iter, kv.size());

  for (int i = 0; i < TEST_SIZE; ++i) {
    int op;
    uint64_t key;
    uint64_t value;
    uint64_t right_value;
    Status s;
    f >> key;
    f_op >> op;
    if (op % 100 == 0) {
      // SCAN
      auto right_iter = right_kv.lower_bound(key);
      auto iter = db->begin();
      iter->Seek(key);
      while (right_iter != right_kv.end()) {
        uint64_t right_key, right_value, get_key;
        get_key = iter->key();
        right_key = right_iter->first;
        value = iter->value();
        right_value = right_iter->second;
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
        value = rnd.Next();
        if (right_kv.count(key)) {
          s = db->Insert(key, value);
          assert(s == Status::ALREADY_EXISTS);
        } else {
          right_kv.emplace(key, value);
          s = db->Insert(key, value);
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
          s = db->Delete(key);
          assert(s == Status::OK);
        } else {
          s = db->Delete(key);
          assert(s == Status::DOES_NOT_EXIST);
        }
        break;
    }
    assert(db->Size() == right_kv.size());
  }

  pop.close();

  return 0;
}