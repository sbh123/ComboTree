#include <iostream>
#include <filesystem>
#include <fstream>
#include <map>
#include "combo-tree/combo_tree.h"
#include "random.h"

using namespace std;

#define COMBO_TREE_DIR  "/mnt/pmem0/combotree/"
#define TEST_SIZE       10000000

int main(void) {
  std::filesystem::remove_all(COMBO_TREE_DIR);
  std::filesystem::create_directory(COMBO_TREE_DIR);

  combotree::ComboTree* db;
  db = new combotree::ComboTree(COMBO_TREE_DIR, PMEMOBJ_MIN_POOL * 128);

  combotree::RandomUniformUint64 rnd;
  map<uint64_t, uint64_t> right_kv;

  fstream f("/home/qyzhang/Projects/ComboTree/build/workload.txt", ios::in);
  fstream f_op("/home/qyzhang/Projects/ComboTree/build/workload1.txt", ios::in);

  uint64_t key, value, right_value;
  int op;

  for (int i = 0; i < TEST_SIZE; ++i) {
    key = rnd.Next();
    db->Insert(key, key);
  }

  for (int i = 0; i < TEST_SIZE; ++i) {
    key = rnd.Next();
    db->Get(key, value);
  }

  // for (int i = 0; i < TEST_SIZE; ++i) {
  //   f_op >> op;
  //   f >> key;
  //   bool res;
  //   switch (op % 4) {
  //     case 0: // PUT
  //     case 1: // GET
  //     case 3:
  //     case 2:
  //       f_op >> value;
  //       if (right_kv.count(key)) {
  //         res = db->Insert(key, value);
  //         assert(!res);
  //       } else {
  //         right_kv.emplace(key, value);
  //         res = db->Insert(key, value);
  //         assert(res);
  //       }
  //       break;
  //       // if (right_kv.count(key)) {
  //       //   right_value = right_kv.at(key);
  //       //   res = db->Get(key, value);
  //       //   assert(res && right_value == value);
  //       // } else {
  //       //   res = db->Get(key, value);
  //       //   assert(!res);
  //       // }
  //       // break;
  //   }
  // }

  // for (int i = 0; i < TEST_SIZE; ++i) {
  //   int op = rnd.Next();
  //   uint64_t key = rnd.Next();
  //   uint64_t value;
  //   uint64_t right_value;
  //   bool res;
  //   switch (op % 4) {
  //     case 0: // PUT
  //     case 1:
  //       value = rnd.Next();
  //       if (right_kv.count(key)) {
  //         res = db->Insert(key, value);
  //         assert(!res);
  //       } else {
  //         right_kv.emplace(key, value);
  //         res = db->Insert(key, value);
  //         assert(res);
  //       }
  //       break;
  //     case 2: // GET
  //     case 3:
  //       if (right_kv.count(key)) {
  //         right_value = right_kv.at(key);
  //         res = db->Get(key, value);
  //         assert(res && right_value == value);
  //       } else {
  //         res = db->Get(key, value);
  //         assert(!res);
  //       }
  //       break;
  //     // case 2: // DELETE
  //       // if (right_kv.count(key)) {
  //       //   right_kv.erase(key);
  //       //   res = db->Delete(key);
  //       //   assert(res);
  //       // } else {
  //       //   res = db->Delete(key);
  //       //   assert(!res);
  //       // }
  //       // break;
  //     // case 3: // UPDATE
  //       // break;
  //   }
  // }
}