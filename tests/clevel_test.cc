#include <map>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>
#include "clevel.h"
#include "slab.h"
#include "random.h"

using combotree::CLevel;
using combotree::Status;
using combotree::Slab;

using namespace std;

#define PATH        "/mnt/pmem0/persistent"
#define TEST_SIZE   3000000

void ScanCallback(uint64_t key, uint64_t value, void* arg) {
  ((std::vector<std::pair<uint64_t,uint64_t>>*)arg)->emplace_back(key, value);
}

int main(void) {
  std::filesystem::remove(PATH);
  auto pop = pmem::obj::pool_base::create(PATH, "CLevel Test",
                                          PMEMOBJ_MIN_POOL * 128, 0666);
  Slab<CLevel::LeafNode>* slab = new Slab<CLevel::LeafNode>(pop, 1000);
  CLevel::MemoryManagement mem(pop, slab);
  CLevel* db;
  db = new CLevel();
  db->InitLeaf(&mem);

  combotree::RandomUniformUint64 rnd;
  std::map<uint64_t, uint64_t> right_kv;

  fstream f("/home/qyzhang/Projects/ComboTree/build/workload.txt", ios::in);
  fstream f_op("/home/qyzhang/Projects/ComboTree/build/workload1.txt", ios::in);

  int put_cnt = 0;
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
      uint64_t size = 0;
      std::vector<std::pair<uint64_t,uint64_t>> kv_pair;
      db->Scan(&mem, key, UINT64_MAX, UINT64_MAX, size, ScanCallback, &kv_pair);
      int j = 0;
      while (right_iter != right_kv.end()) {
        assert(j < kv_pair.size());
        assert(right_iter->first == kv_pair[j].first);
        assert(right_iter->second == kv_pair[j].second);
        right_iter++;
        j++;
      }
      assert(j == kv_pair.size());
    }
    switch (op % 3) {
      case 0: // PUT
        value = key;
        if (right_kv.count(key)) {
          s = db->Insert(&mem, key, value);
          assert(s == Status::ALREADY_EXISTS);
        } else {
          // if (put_cnt >= 14)
          //   break;
          right_kv.emplace(key, value);
          s = db->Insert(&mem, key, value);
          assert(s == Status::OK);
          put_cnt++;
        }
        break;
      case 1: // GET
        if (right_kv.count(key)) {
          right_value = right_kv.at(key);
          s = db->Get(&mem, key, value);
          assert(s == Status::OK && right_value == value);
        } else {
          s = db->Get(&mem, key, value);
          assert(s == Status::DOES_NOT_EXIST);
        }
        break;
      case 2: // DELETE
        if (right_kv.count(key)) {
          right_kv.erase(key);
          s = db->Delete(&mem, key);
          assert(s == Status::OK);
        } else {
          s = db->Delete(&mem, key);
          assert(s == Status::DOES_NOT_EXIST);
        }
        break;
    }
  }

  return 0;
}