#include <iostream>
#include <filesystem>
#include <fstream>
#include <map>
#include "alevel.h"
#include "combotree/iterator.h"
#include "std_map_iterator.h"
#include "random.h"

using namespace std;
using combotree::Status;

#define TEST_SIZE 200000

#define POOL_PATH   "/mnt/pmem0/persistent"
#define POOL_LAYOUT "Combo Tree"
#define POOL_SIZE   (1UL << 30) /* 1G */

int main(void) {
  std::filesystem::remove(POOL_PATH);
  auto pop = pmem::obj::pool_base::create(POOL_PATH, POOL_LAYOUT, POOL_SIZE, 0666);

  std::map<uint64_t, uint64_t> kv;
  std::map<uint64_t, uint64_t> right_kv;
  combotree::RandomUniformUint64 rnd;

  fstream f("/home/qyzhang/Projects/ComboTree/build/workload.txt", ios::in);

  uint64_t key;
  for (int i = 0; i < 1024; ++i) {
    f >> key;
    kv.emplace(key, key);
    right_kv.emplace(key, key);
  }

  combotree::Iterator* iter = new combotree::MapIterator(&kv);
  iter->SeekToFirst();
  combotree::BLevel* blevel = new combotree::BLevel(pop, iter, kv.size());
  combotree::ALevel* db = new combotree::ALevel(blevel);

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
    Status s;
    switch (op % 4) {
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

  pop.close();

  return 0;
}