#include <iostream>
#include <fstream>
#include <cassert>
#include <iomanip>
#include <sstream>
#include <thread>
#include <getopt.h>
#include <unistd.h>
#include "combotree/combotree.h"
#include "combotree_config.h"
#include "random.h"
#include "timer.h"

size_t LOAD_SIZE   = 10000000;
size_t PUT_SIZE    = 6000000;
size_t GET_SIZE    = 1000000;
size_t DELETE_SIZE = 1000000;
size_t SCAN_TEST_SIZE = 500000000;

int thread_num        = 4;
bool use_data_file    = false;
std::vector<size_t> scan_size;
std::vector<size_t> sort_scan_size;

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

void show_help(char* prog) {
  std::cout <<
    "Usage: " << prog << " [options]" << std::endl <<
    std::endl <<
    "  Option:" << std::endl <<
    "    --thread[-t]             thread number" << std::endl <<
    "    --load-size              LOAD_SIZE" << std::endl <<
    "    --put-size               PUT_SIZE" << std::endl <<
    "    --get-size               GET_SIZE" << std::endl <<
    "    --delete-size            DELETE_SIZE" << std::endl <<
    "    --scan-test-size         SCAN_TEST_SIZE" << std::endl <<
    "    --scan[-s]               add scan" << std::endl <<
    "    --sort-scan              add sort scan" << std::endl <<
    "    --use-data-file[-d]      use data file" << std::endl <<
    "    --help[-h]               show help" << std::endl;
}

int main(int argc, char** argv) {
  static struct option opts[] = {
  /* NAME               HAS_ARG            FLAG  SHORTNAME*/
    {"thread",          required_argument, NULL, 't'},
    {"load-size",       required_argument, NULL, 0},
    {"put-size",        required_argument, NULL, 0},
    {"get-size",        required_argument, NULL, 0},
    {"delete-size",     required_argument, NULL, 0},
    {"scan-test-size",  required_argument, NULL, 0},
    {"scan",            required_argument, NULL, 's'},
    {"sort-scan",       required_argument, NULL, 0},
    {"use-data-file",   no_argument,       NULL, 'd'},
    {"help",            no_argument,       NULL, 'h'},
    {NULL, 0, NULL, 0}
  };

  int c;
  int opt_idx;
  while ((c = getopt_long(argc, argv, "t:s:dh", opts, &opt_idx)) != -1) {
    switch (c) {
      case 0:
        switch (opt_idx) {
          case 0: thread_num = atoi(optarg); break;
          case 1: LOAD_SIZE = atoi(optarg); break;
          case 2: PUT_SIZE = atoi(optarg); break;
          case 3: GET_SIZE = atoi(optarg); break;
          case 4: DELETE_SIZE = atoi(optarg); break;
          case 5: SCAN_TEST_SIZE = atoi(optarg); break;
          case 6: scan_size.push_back(atoi(optarg)); break;
          case 7: sort_scan_size.push_back(atoi(optarg)); break;
          case 8: use_data_file = true; break;
          case 9: show_help(argv[0]); return 0;
          default: std::cerr << "Parse Argument Error!" << std::endl; abort();
        }
        break;
      case 't': thread_num = atoi(optarg); break;
      case 's': scan_size.push_back(atoi(optarg)); break;
      case 'd': use_data_file = true; break;
      case 'h': show_help(argv[0]); return 0;
      case '?': break;
      default:  std::cout << (char)c << std::endl; abort();
    }
  }

  if (GET_SIZE > LOAD_SIZE+PUT_SIZE) {
    std::cerr << "GET_SIZE > TEST_SIZE+PUT_SIZE!" << std::endl;
    return -1;
  }
  if (DELETE_SIZE > LOAD_SIZE+PUT_SIZE) {
    std::cerr << "DELETE_SIZE > TEST_SIZE+PUT_SIZE!" << std::endl;
    return -1;
  }

  std::cout << "THREAD NUMBER:         " << thread_num << std::endl;
  std::cout << "LOAD_SIZE:             " << LOAD_SIZE << std::endl;
  std::cout << "PUT_SIZE:              " << PUT_SIZE << std::endl;
  std::cout << "GET_SIZE:              " << GET_SIZE << std::endl;
  std::cout << "DELETE_SIZE:           " << DELETE_SIZE << std::endl;
  std::cout << "SCAN_TEST_SIZE:        " << SCAN_TEST_SIZE << std::endl;
  for (auto &sz : scan_size)
    std::cout << "SCAN:                  " << sz << std::endl;
  for (auto &sz : sort_scan_size)
    std::cout << "SORT_SCAN:             " << sz << std::endl;
  std::cout << std::endl;

  std::vector<uint64_t> key;

  if (use_data_file) {
    std::ifstream data("./data.dat");
    if (!data.good()) {
      std::cout << "can not open data.dat!" << std::endl;
      assert(0);
    }
    for (size_t i = 0; i < LOAD_SIZE+PUT_SIZE; ++i) {
      uint64_t k;
      data >> k;
      key.push_back(k);
    }
    std::cout << "finish read data.dat" << std::endl;
  } else {
    Random rnd(0, LOAD_SIZE+PUT_SIZE-1);
    for (size_t i = 0; i < LOAD_SIZE+PUT_SIZE; ++i)
      key.push_back(i);
    for (size_t i = 0; i < LOAD_SIZE+PUT_SIZE; ++i)
      std::swap(key[i],key[rnd.Next()]);
  }

#ifdef SERVER
  ComboTree* tree = new ComboTree("/pmem0/combotree/", (1024*1024*1024*100UL), true);
#else
  ComboTree* tree = new ComboTree("/mnt/pmem0/", (1024*1024*512UL), true);
#endif

  Timer timer;
  std::vector<std::thread> threads;
  size_t per_thread_size;

  std::cout << std::fixed << std::setprecision(2);

  int pid = getpid();
  char cmd_buf[100];
  sprintf(cmd_buf, "pmap %d > ./usage_before.txt", pid);
  if (system(cmd_buf) != 0) {
    std::cerr << "command error: " << cmd_buf << std::endl;
    return -1;
  }

  // Load
  per_thread_size = LOAD_SIZE / thread_num;
  timer.Record("start");
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([=,&key](){
      size_t start_pos = i*per_thread_size;
      size_t size = (i == thread_num-1) ? LOAD_SIZE-(thread_num-1)*per_thread_size : per_thread_size;
      for (size_t j = 0; j < size; ++j) {
        bool ret = tree->Put(key[start_pos+j], key[start_pos+j]);
        if (ret != true) {
          std::cout << "load error!" << std::endl;
          assert(0);
        }
      }
    });
  }
  for (auto& t : threads)
    t.join();
  timer.Record("stop");
  threads.clear();
  uint64_t total_time = timer.Microsecond("stop", "start");
  std::cout << "load: " << total_time/1000000.0 << " " << (double)LOAD_SIZE/(double)total_time*1000000.0 << std::endl;

  // Put
  per_thread_size = PUT_SIZE / thread_num;
  timer.Clear();
  timer.Record("start");
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([=,&key](){
      size_t start_pos = i*per_thread_size+LOAD_SIZE;
      size_t size = (i == thread_num-1) ? PUT_SIZE-(thread_num-1)*per_thread_size : per_thread_size;
      for (size_t j = 0; j < size; ++j) {
        bool ret = tree->Put(key[start_pos+j], key[start_pos+j]);
        if (ret != true) {
          std::cout << "put error!" << std::endl;
          assert(0);
        }
      }
    });
  }
  for (auto& t : threads)
    t.join();
  timer.Record("stop");
  threads.clear();
  total_time = timer.Microsecond("stop", "start");
  std::cout << "put:  " << total_time/1000000.0 << " " << (double)PUT_SIZE/(double)total_time*1000000.0 << std::endl;

  // statistic
  std::cout << "clevel time:    " << tree->CLevelTime()/1000000.0 << std::endl;
  std::cout << "entries:        " << tree->BLevelEntries() << std::endl;
  std::cout << "clevels:        " << tree->CLevelCount() << std::endl;
  std::cout << "clevel percent: " << (double)tree->CLevelCount() / tree->BLevelEntries() * 100.0 << "%" << std::endl;
  std::cout << "size:           " << tree->Size() << std::endl;
  std::cout << "usage:          " << human_readable(tree->Usage()) << std::endl;
  std::cout << "bytes-per-pair: " << (double)tree->Usage() / tree->Size() << std::endl;
  tree->BLevelCompression();

  sprintf(cmd_buf, "pmap %d > ./usage_after.txt", pid);
  if (system(cmd_buf) != 0) {
    std::cerr << "command error: " << cmd_buf << std::endl;
    return -1;
  }

  // Get
  Random get_rnd(0, GET_SIZE-1);
  for (size_t i = 0; i < GET_SIZE; ++i)
    std::swap(key[i],key[get_rnd.Next()]);
  per_thread_size = GET_SIZE / thread_num;
  timer.Clear();
  timer.Record("start");
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([=,&key](){
      size_t start_pos = i*per_thread_size;
      size_t size = (i == thread_num-1) ? GET_SIZE-(thread_num-1)*per_thread_size : per_thread_size;
      size_t value;
      for (size_t j = 0; j < size; ++j) {
        bool ret = tree->Get(key[start_pos+j], value);
        if (ret != true) {
          std::cout << "get error!" << std::endl;
          assert(0);
        }
      }
    });
  }
  for (auto& t : threads)
    t.join();
  timer.Record("stop");
  threads.clear();
  total_time = timer.Microsecond("stop", "start");
  std::cout << "get: " << total_time/1000000.0 << " " << (double)GET_SIZE/(double)total_time*1000000.0 << std::endl;

  // scan
  for (auto scan : scan_size) {
    size_t total_size = std::min(SCAN_TEST_SIZE / scan, LOAD_SIZE+PUT_SIZE);
    per_thread_size = total_size / thread_num;
    timer.Clear();
    timer.Record("start");
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([=,&key](){
        size_t start_pos = i*per_thread_size;
        size_t size = (i == thread_num-1) ? total_size-(thread_num-1)*per_thread_size : per_thread_size;
        for (size_t j = 0; j < size; ++j) {
          uint64_t start_key = key[start_pos+j];
          ComboTree::NoSortIter iter(tree, start_key);
          if (iter.end())
            continue;
          for (size_t k = 0; k < scan; ++k) {
            if (iter.key() != iter.value()) {
              std::cout << "scan error" << std::endl;
              assert(0);
            }
            if (!iter.next())
              break;
          }
        }
      });
    }
    for (auto& t : threads)
      t.join();
    timer.Record("stop");
    threads.clear();
    total_time = timer.Microsecond("stop", "start");
    std::cout << "scan " << scan << ": " << total_time/1000000.0 << " " << (double)total_size/(double)total_time*1000000.0 << std::endl;
  }

  // sort_scan
  for (auto scan : sort_scan_size) {
    size_t total_size = std::min(SCAN_TEST_SIZE / scan, LOAD_SIZE+PUT_SIZE);
    per_thread_size = total_size / thread_num;
    timer.Clear();
    timer.Record("start");
    for (int i = 0; i < thread_num; ++i) {
      threads.emplace_back([=,&key](){
        size_t start_pos = i*per_thread_size;
        size_t size = (i == thread_num-1) ? total_size-(thread_num-1)*per_thread_size : per_thread_size;
        for (size_t j = 0; j < size; ++j) {
          uint64_t start_key = key[start_pos+j];
          ComboTree::Iter iter(tree, start_key);
          if (iter.end())
            continue;
          for (size_t k = 0; k < scan; ++k) {
            if (iter.key() != start_key + k) {
              std::cout << "scan error!" << std::endl;
              assert(0);
            }
            if (iter.value() != start_key + k) {
              std::cout << "scan error!" << std::endl;
              assert(0);
            }
            if (!iter.next())
              break;
          }
        }
      });
    }
    for (auto& t : threads)
      t.join();
    timer.Record("stop");
    threads.clear();
    total_time = timer.Microsecond("stop", "start");
    std::cout << "sort scan " << scan << ": " << total_time/1000000.0 << " " << (double)total_size/(double)total_time*1000000.0 << std::endl;
  }

  // Delete
  Random delete_rnd(0, DELETE_SIZE-1);
  for (size_t i = 0; i < DELETE_SIZE; ++i)
    std::swap(key[i],key[delete_rnd.Next()]);
  per_thread_size = DELETE_SIZE / thread_num;
  timer.Clear();
  timer.Record("start");
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([=,&key](){
      size_t start_pos = i*per_thread_size;
      size_t size = (i == thread_num-1) ? DELETE_SIZE-(thread_num-1)*per_thread_size : per_thread_size;
      for (size_t j = 0; j < size; ++j) {
        if (tree->Delete(key[start_pos+j]) != true) {
          std::cout << "delete error!" << std::endl;
          assert(0);
        }
      }
    });
  }
  for (auto& t : threads)
    t.join();
  timer.Record("stop");
  threads.clear();
  total_time = timer.Microsecond("stop", "start");
  std::cout << "delete: " << total_time/1000000.0 << " " << (double)DELETE_SIZE/(double)total_time*1000000.0 << std::endl;

  delete tree;

  return 0;
}