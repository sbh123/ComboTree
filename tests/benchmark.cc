#include <iostream>
#include <fstream>
#include <cassert>
#include <iomanip>
#include <sstream>
#include "combotree/combotree.h"
#include "combotree_config.h"
#include "random.h"
#include "timer.h"

#define TEST_SIZE       10000000
#define LAST_EXPAND     600000
#define GET_SIZE        1000000
#define SCAN_TEST_SIZE  1000000

bool use_data_file = true;
int SCAN_SIZE = 100;

using combotree::ComboTree;
using combotree::Random;
using combotree::Timer;

// return human readable string of size
std::string human_readable(double size) {
  static const std::string suffix[] = {
    "B",
    "KB",
    "MB",
    "GB"
  };
  const int arr_len = 4;

  std::ostringstream out;
  out.precision(2);
  for (int divs = 0; divs < arr_len; ++divs) {
    if (size >= 1024.0) {
      size /= 1024.0;
    } else {
      out << std::fixed << size;
      return out.str() + suffix[divs];
    }
  }
  out << std::fixed << size;
  return out.str() + suffix[arr_len - 1];
}

int main(int argc, char** argv) {
#ifdef SERVER
  ComboTree* tree = new ComboTree("/pmem0/combotree/", (1024*1024*1024*100UL), true);
#else
  ComboTree* tree = new ComboTree("/mnt/pmem0/", (1024*1024*512UL), true);
#endif

  if (argc == 2) {
    SCAN_SIZE = atoi(argv[1]);
  }

  std::cout << "TEST_SIZE:             " << TEST_SIZE << std::endl;
  std::cout << "LAST_EXPAND:           " << LAST_EXPAND << std::endl;
  std::cout << "BLEVEL_EXPAND_BUF_KEY: " << BLEVEL_EXPAND_BUF_KEY << std::endl;
  std::cout << "EXPANSION_FACTOR:      " << EXPANSION_FACTOR << std::endl;
  std::cout << "PMEMKV_THRESHOLD:      " << PMEMKV_THRESHOLD << std::endl;
  std::cout << "SCAN_SIZE:             " << SCAN_SIZE << std::endl;

#ifdef STREAMING_STORE
  std::cout << "STREAMING_STORE = 1" << std::endl;
#endif

#ifdef STREAMING_LOAD
  std::cout << "STREAMING_LOAD  = 1" << std::endl;
#endif

  std::vector<uint64_t> key;

  if (use_data_file) {
    std::ifstream data("./data.dat");

    for (int i = 0; i < TEST_SIZE; ++i) {
      uint64_t k;
      data >> k;
      key.push_back(k);
      assert(data.good());
    }
  } else {
    Random rnd(0, TEST_SIZE-1);
    for (int i = 0; i < TEST_SIZE; ++i)
      key.push_back(i);
    for (int i = 0; i < TEST_SIZE; ++i)
      std::swap(key[i],key[rnd.Next()]);
  }

  uint64_t value;
  Timer timer;

  // Put
  timer.Record("start");
  for (size_t i = 0; i < LAST_EXPAND; ++i)
    assert(tree->Put(key[i], key[i]) == true);
  timer.Record("mid");
  for (size_t i = LAST_EXPAND; i < TEST_SIZE; ++i)
    assert(tree->Put(key[i], key[i]) == true);
  timer.Record("stop");

  uint64_t total_time = timer.Microsecond("mid", "start");
  std::cout << "load: " << total_time/1000000.0 << " " << (double)TEST_SIZE/total_time*1000000.0 << std::endl;
  uint64_t mid_time = timer.Microsecond("stop", "mid");
  std::cout << "put:  " << mid_time/1000000.0 << " " << (double)(TEST_SIZE-LAST_EXPAND)/mid_time*1000000.0 << std::endl;

  std::cout << "clevel time:    " << tree->CLevelTime()/1000000.0 << std::endl;

  std::cout << std::fixed << std::setprecision(2);

  std::cout << "entries:        " << tree->BLevelEntries() << std::endl;
  std::cout << "clevels:        " << tree->CLevelCount() << std::endl;
  std::cout << "clevel percent: " << (double)tree->CLevelCount() / tree->BLevelEntries() * 100.0 << "%" << std::endl;
  std::cout << "size:           " << tree->Size() << std::endl;
  std::cout << "usage:          " << human_readable(tree->Usage()) << std::endl;
  std::cout << "bytes-per-pair: " << (double)tree->Usage() / tree->Size() << std::endl;
  tree->BLevelCompression();

  // Get
  timer.Clear();
  timer.Record("start");
  for (int i = 0; i < GET_SIZE; ++i) {
    uint64_t target = key[i];
    assert(tree->Get(target, value) == true);
    assert(value == target);
  }
  timer.Record("stop");
  total_time = timer.Microsecond("stop", "start");
  std::cout << "get: " << total_time/1000000.0 << " " << (double)GET_SIZE/(double)total_time*1000000.0 << std::endl;

  for (uint64_t i = TEST_SIZE; i < TEST_SIZE+10000; ++i) {
    assert(tree->Get(i, value) == false);
  }

  // scan
  timer.Clear();
  timer.Record("start");
  for (int i = 0; i < SCAN_TEST_SIZE; ++i) {
    uint64_t start_key = key[i];
    ComboTree::Iter iter(tree, start_key);
    for (int j = 0; j < SCAN_SIZE; ++j) {
      assert(iter.key() == start_key + j);
      assert(iter.value() == start_key + j);
      if (!iter.next())
        break;
    }
  }
  timer.Record("stop");
  total_time = timer.Microsecond("stop", "start");
  std::cout << "scan " << SCAN_SIZE << ": " << total_time/1000000.0 << " " << (double)SCAN_TEST_SIZE/(double)total_time*1000000.0 << std::endl;

  // Delete
  // for (auto& k : key) {
  //   assert(tree->Delete(k) == true);
  // }
  // for (auto& k : key) {
  //   assert(tree->Get(k, value) == false);
  // }

  return 0;
}