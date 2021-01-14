#include <iostream>
#include <cassert>
#include <iomanip>
#include <map>
#include "combotree/combotree.h"
#include "../src/combotree_config.h"
#include "nvm_alloc.h"
#include "random.h"

#define TEST_SIZE   4000000
// #define TEST_SIZE   3000


using combotree::ComboTree;
using combotree::Random;

int main(void) {
#ifdef SERVER
  ComboTree* tree = new ComboTree("/pmem0/combotree/", (1024*1024*1024*100UL), true);
#else
  ComboTree* tree = new ComboTree("/mnt/pmem0/", (1024*1024*512UL), true);
#endif

  std::cout << "TEST_SIZE:             " << TEST_SIZE << std::endl;

  std::vector<uint64_t> key;
  std::map<uint64_t, uint64_t> right_kv;

  Random rnd(0, UINT64_MAX - 1);
  NVM::env_init();
  for (uint64_t i = 0; i < TEST_SIZE; ++i) {
    uint64_t key = rnd.Next();
    if (right_kv.count(key)) {
      i--;
      continue;
    }
    uint64_t value = rnd.Next();
    right_kv.emplace(key, value);
    tree->Put(key, value);
  }

  uint64_t value;
  // {
  //   ComboTree::Iter it(tree, 0);
  //   for(int i = 0;i < 100; ++ i, it.next()) {
  //     std::cout << "iter " << i << ", key " << it.key() << ", value " << it.value() << std::endl;
  //   }
  // }
  // Get
  for (auto &kv : right_kv) {
    assert(tree->Get(kv.first, value) == true);
    if(value != kv.second) {
      std::cout << "key: " << kv.first << ", find " << value << ", expect " << kv.second << std::endl;
      tree->Get(kv.first, value);
      assert(value == kv.second);
    }
  }

  for (uint64_t i = 0; i < TEST_SIZE * 2; ++i) {
    if (right_kv.count(i) == 0) {
      assert(tree->Get(i, value) == false);
    }
  }

  // scan
  auto right_iter = right_kv.begin();
  ComboTree::Iter iter(tree);
  while (right_iter != right_kv.end()) {
    assert(right_iter->first == iter.key());
    assert(right_iter->second == iter.value());
    right_iter++;
    iter.next();
  }
  assert(iter.end());

  for (int i = 0; i < 1000; ++i) {
    uint64_t start_key = rnd.Next();
    auto right_iter = right_kv.lower_bound(start_key);
    ComboTree::Iter iter(tree, start_key);
    // ComboTree::Iter riter(tree);
    // while (riter.key() < start_key)
    //   riter.next();
    // assert(right_iter->first == riter.key());
    if (right_iter->first != iter.key()) {
      // b combotree_test.cc:79 if i==122
      // b blevel.h:121
      // b bentry.h:77
      // b tmp_bentry.h:99
      // b clevel.h:180
      std::cout << i << "key: " << start_key << ", find " << iter.key() << ", expect " << right_iter->first << std::endl;
      ComboTree::Iter iter(tree, start_key);
      assert(right_iter->first == iter.key());
    }
    for (int j = 0; j < 100 && right_iter != right_kv.cend(); ++j) {
      assert(!iter.end());
      // assert(!riter.end());
      // uint64_t rkey = riter.key();
      // uint64_t rvalue = riter.value();
      // uint64_t k = iter.key();
      // uint64_t v = iter.value();
      // assert(right_iter->first == riter.key());
      // assert(right_iter->second == riter.value());
      assert(right_iter->first == iter.key());
      assert(right_iter->second == iter.value());
      right_iter++;
      iter.next();
      // riter.next();
    }
  }

  // NoSort Scan
  {
    std::vector<uint64_t> scan_keys;
    {
      ComboTree::NoSortIter no_sort_iter(tree, 100);
      for (int i = 0; i < 100; ++i) {
        // std::cout << no_sort_iter.key() << " " << no_sort_iter.value() << std::endl;
        scan_keys.push_back(no_sort_iter.key());
        assert(no_sort_iter.next());
      }
    }

    std::cout << "Scan finished. start Update." << std::endl;
    for (int i = 0; i < 100; ++i) {
      tree->Update(scan_keys[i], scan_keys[i] + 1);
    }
    std::cout << "Update finished." << std::endl;
  }

  {
    // Sort Scan
    std::vector<uint64_t> scan_keys;
    {
      ComboTree::Iter sort_iter(tree, 100);
      for (int i = 0; i < 100; ++i) {
        // std::cout << sort_iter.key() << " " << sort_iter.value() << std::endl;
        scan_keys.push_back(sort_iter.key());
        assert(sort_iter.next());
      }
    }
    std::cout << "Scan finished. start Update." << std::endl;
    for (int i = 0; i < 100; ++i) {
      tree->Update(scan_keys[i], scan_keys[i] + 1);
    }
    std::cout << "Update finished." << std::endl;
  }
  NVM::env_exit();
  return 0;
}