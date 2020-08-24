#include <iostream>
#include <filesystem>
#include <fstream>
#include <map>
#include <vector>
#include <cassert>
#include "combotree/combotree.h"
#include "random.h"

using namespace std;

#define COMBO_TREE_DIR  "/mnt/pmem0/combotree/"
#define TEST_SIZE       40000000

int main(void) {
  std::filesystem::remove_all(COMBO_TREE_DIR);
  std::filesystem::create_directory(COMBO_TREE_DIR);

  combotree::ComboTree* db;
  db = new combotree::ComboTree(COMBO_TREE_DIR, PMEMOBJ_MIN_POOL * 128);

  combotree::RandomUniformUint64 rnd;
  map<uint64_t, uint64_t> right_kv;

  fstream f("/home/qyzhang/Projects/ComboTree/build/workload.txt", ios::in);
  fstream f_op("/home/qyzhang/Projects/ComboTree/build/workload1.txt", ios::in);

  for (int i = 0; i < TEST_SIZE; ++i) {
    int op;
    uint64_t key;
    uint64_t value;
    uint64_t right_value;
    // f_op >> op;
    // f >> key;
    key = rnd.Next();
    op = rnd.Next();
    bool res;
    if (op % 100 == 0) {
      // SCAN
      std::vector<std::pair<uint64_t,uint64_t>> results;
      size_t size = 0;
      size = db->Scan(key, UINT64_MAX, UINT64_MAX, results);
      auto right_iter = right_kv.lower_bound(key);
      auto iter = results.begin();
      assert(size == results.size());
      int cnt = 0;
      while (right_iter != right_kv.end()) {
        cnt++;
        assert(iter != results.end());
        assert(right_iter->first == iter->first);
        assert(right_iter->second == iter->second);
        right_iter++;
        iter++;
      }
      assert(iter == results.end());
    }
    switch (op % 1) {
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
    }
  }
}