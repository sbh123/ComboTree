#include <iostream>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include "alevel.h"
#include "random.h"

using namespace std;
using combotree::Status;

#define TEST_SIZE 40000000

#define POOL_PATH   "/mnt/pmem0/persistent"
#define POOL_LAYOUT "Combo Tree"
#define POOL_SIZE   (1UL << 30) /* 1G */

int main(void) {
  std::filesystem::remove(POOL_PATH);
  auto pop = pmem::obj::pool_base::create(POOL_PATH, POOL_LAYOUT, POOL_SIZE, 0666);

  std::vector<std::pair<uint64_t, uint64_t>> kv;
  std::map<uint64_t, uint64_t> right_kv;
  combotree::RandomUniformUint64 rnd;

  fstream f("/home/qyzhang/Projects/ComboTree/build/workload.txt", ios::in);
  fstream f_op("/home/qyzhang/Projects/ComboTree/build/workload1.txt", ios::in);

  uint64_t key;
  for (int i = 0; i < 1000000; ++i) {
    // f >> key;
    key = rnd.Next();
    if (right_kv.count(key)) {
      i--;
      continue;
    }
    kv.emplace_back(key, key);
    right_kv.emplace(key, key);
  }

  std::sort(kv.begin(), kv.end());
  shared_ptr<combotree::BLevel> blevel = make_shared<combotree::BLevel>(pop, kv);
  shared_ptr<combotree::ALevel> db = make_shared<combotree::ALevel>(blevel);

  for (int i = 0; i < TEST_SIZE; ++i) {
    int op;
    uint64_t key;
    uint64_t value;
    uint64_t right_value;
    Status s;
    // f >> key;
    // f_op >> op;
    op = rnd.Next();
    key = rnd.Next();
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
  }

  pop.close();

  return 0;
}