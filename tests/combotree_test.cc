#include <iostream>
#include <filesystem>
#include <fstream>
#include <map>
#include <vector>
#include "combotree/combotree.h"
#include "random.h"

using namespace std;

#define COMBO_TREE_DIR  "/mnt/pmem0/combotree/"
#define TEST_SIZE       1000000

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
      // auto right_iter = right_kv.lower_bound(key);
      // auto iter = db->begin();
      // iter->Seek(key);
      // while (right_iter != right_kv.end()) {
      //   assert(right_iter->first == iter->key());
      //   assert(right_iter->second == iter->value());
      //   right_iter++;
      //   iter->Next();
      // }
      // assert(iter->End());
      // delete iter;
      // continue;
    }
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
    }
  }
}