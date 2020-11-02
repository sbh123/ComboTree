#include <iostream>
#include <cassert>
#include <iomanip>
#include <map>
#include "combotree/combotree.h"
#include "combotree_config.h"
#include "random.h"

#define TEST_SIZE   4000000

using combotree::ComboTree;
using combotree::Random;

int main(void) {
#if SERVER
  ComboTree* tree = new ComboTree("/pmem0/combotree/", (1024*1024*1024*100UL), true);
#else
  ComboTree* tree = new ComboTree("/mnt/pmem0/", (1024*1024*512UL), true);
#endif

  std::cout << "TEST_SIZE:             " << TEST_SIZE << std::endl;
  std::cout << "BLEVEL_EXPAND_BUF_KEY: " << BLEVEL_EXPAND_BUF_KEY << std::endl;
  std::cout << "EXPANSION_FACTOR:      " << EXPANSION_FACTOR << std::endl;
  std::cout << "PMEMKV_THRESHOLD:      " << PMEMKV_THRESHOLD << std::endl;

#if STREAMING_STORE
  std::cout << "STREAMING_STORE = 1" << std::endl;
#endif

#if STREAMING_LOAD
  std::cout << "STREAMING_LOAD  = 1" << std::endl;
#endif

  std::vector<uint64_t> key;
  std::map<uint64_t, uint64_t> right_kv;

  Random rnd(0, UINT64_MAX - 1);
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

  // Get
  for (auto &kv : right_kv) {
    assert(tree->Get(kv.first, value) == true);
    assert(value == kv.second);
  }

  for (uint64_t i = 0; i < TEST_SIZE; ++i) {
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
    assert(right_iter->first == iter.key());
    for (int j = 0; j < 100 && right_iter != right_kv.cend(); ++j) {
      assert(!iter.end());
      assert(right_iter->first == iter.key());
      assert(right_iter->second == iter.value());
      right_iter++;
      iter.next();
      // riter.next();
    }
  }

  return 0;
}