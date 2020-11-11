#include <iostream>
#include <fstream>
#include <cassert>
#include <iomanip>
#include <sstream>
#include <thread>
#include "combotree/combotree.h"
#include "combotree_config.h"
#include "random.h"
#include "timer.h"

size_t TEST_SIZE      = 10000000;
size_t LAST_EXPAND    = 6000000;
size_t GET_SIZE       = 1000000;
size_t SCAN_TEST_SIZE = 1000000;
size_t SCAN_SIZE      = 100;

int thread_num      = 4;
bool use_data_file  = true;

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
#if SERVER
  ComboTree* tree = new ComboTree("/pmem0/combotree/", (1024*1024*1024*100UL), true);
#else
  ComboTree* tree = new ComboTree("/mnt/pmem0/", (1024*1024*512UL), true);
#endif

  if (argc >= 2) {
    SCAN_SIZE = atoi(argv[1]);
  }

  if (argc >= 3) {
    thread_num = atoi(argv[2]);
  }

  TEST_SIZE      = (TEST_SIZE / thread_num) * thread_num;
  LAST_EXPAND    = (LAST_EXPAND / thread_num) * thread_num;
  GET_SIZE       = (GET_SIZE / thread_num) * thread_num;
  SCAN_TEST_SIZE = (SCAN_TEST_SIZE / thread_num) * thread_num;

  std::cout << "TEST_SIZE:             " << TEST_SIZE << std::endl;
  std::cout << "LAST_EXPAND:           " << LAST_EXPAND << std::endl;
  std::cout << "BLEVEL_EXPAND_BUF_KEY: " << BLEVEL_EXPAND_BUF_KEY << std::endl;
  std::cout << "EXPANSION_FACTOR:      " << EXPANSION_FACTOR << std::endl;
  std::cout << "PMEMKV_THRESHOLD:      " << PMEMKV_THRESHOLD << std::endl;
  std::cout << "SCAN_SIZE:             " << SCAN_SIZE << std::endl;

#if STREAMING_STORE
  std::cout << "STREAMING_STORE = 1" << std::endl;
#endif

#if STREAMING_LOAD
  std::cout << "STREAMING_LOAD  = 1" << std::endl;
#endif

  std::vector<uint64_t> key;

  if (use_data_file) {
    std::ifstream data("./data.dat");
    for (size_t i = 0; i < TEST_SIZE; ++i) {
      uint64_t k;
      data >> k;
      key.push_back(k);
      assert(data.good());
    }
  } else {
    Random rnd(0, TEST_SIZE-1);
    for (size_t i = 0; i < TEST_SIZE; ++i)
      key.push_back(i);
    for (size_t i = 0; i < TEST_SIZE; ++i)
      std::swap(key[i],key[rnd.Next()]);
  }

  uint64_t value;
  Timer timer;

  std::vector<std::thread> threads;
  size_t per_thread_size;

  // LOAD
  per_thread_size = LAST_EXPAND / thread_num;
  timer.Record("start");
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([=,&key](){
      size_t start_pos = i*per_thread_size;
      for (size_t j = 0; j < per_thread_size; ++j)
        assert(tree->Put(key[start_pos+j], key[start_pos+j]) == true);
    });
  }
  for (auto& t : threads)
    t.join();
  threads.clear();

  timer.Record("mid");
  per_thread_size = (TEST_SIZE - LAST_EXPAND) / thread_num;
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([=,&key](){
      size_t start_pos = i*per_thread_size+LAST_EXPAND;
      for (size_t j = 0; j < per_thread_size; ++j)
        assert(tree->Put(key[start_pos+j], key[start_pos+j]) == true);
    });
  }
  for (auto& t : threads)
    t.join();
  threads.clear();
  timer.Record("stop");

  uint64_t total_time = timer.Microsecond("mid", "start");
  std::cout << "load: " << total_time/1000000.0 << " " << (double)TEST_SIZE/(double)total_time*1000000.0 << std::endl;
  uint64_t mid_time = timer.Microsecond("stop", "mid");
  std::cout << "put:  " << mid_time/1000000.0 << " " << (double)(TEST_SIZE-LAST_EXPAND)/(double)mid_time*1000000.0 << std::endl;

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
  per_thread_size = GET_SIZE / thread_num;
  timer.Clear();
  timer.Record("start");
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([=,&key](){
      size_t start_pos = i*per_thread_size;
      size_t value;
      for (size_t j = 0; j < per_thread_size; ++j) {
        assert(tree->Get(key[start_pos+j], value) == true);
        assert(value == key[start_pos+j]);
      }
    });
  }
  for (auto& t : threads)
    t.join();
  threads.clear();
  timer.Record("stop");
  total_time = timer.Microsecond("stop", "start");
  std::cout << "get: " << total_time/1000000.0 << " " << (double)GET_SIZE/(double)total_time*1000000.0 << std::endl;

  for (size_t i = TEST_SIZE; i < TEST_SIZE+10000; ++i) {
    assert(tree->Get(i, value) == false);
  }

  // scan
  timer.Clear();
  per_thread_size = SCAN_TEST_SIZE / thread_num;
  timer.Record("start");
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([=,&key](){
      size_t start_pos = i*per_thread_size;
      for (size_t j = 0; j < per_thread_size; ++j) {
        uint64_t start_key = key[start_pos+j];
        ComboTree::Iter iter(tree, start_key);
        for (size_t k = 0; k < SCAN_SIZE; ++k) {
          assert(iter.key() == start_key + k);
          assert(iter.value() == start_key + k);
          if (!iter.next())
            break;
        }
      }
    });
  }
  for (auto& t : threads)
    t.join();
  threads.clear();
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