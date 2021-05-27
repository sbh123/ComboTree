#include <iostream>
#include <fstream>
#include <cassert>
#include <iomanip>
#include <sstream>
#include <thread>
#include <getopt.h>
#include <unistd.h>
#include <cmath>
#include "combotree/combotree.h"
#include "../src/combotree_config.h"
#include "distribute.h"
#include "db_interface.h"
#include "random.h"
#include "nvm_alloc.h"
#include "timer.h"

size_t LOAD_SIZE   = 10000000;
size_t PUT_SIZE    = 6000000;
size_t GET_SIZE    = 1000000;
size_t DELETE_SIZE = 1000000;
size_t SCAN_TEST_SIZE = 500000000;
size_t load_tests[] = {
    10000000,
    50000000,
    100000000,
    200000000,
    400000000 };

#define ArrayLen(arry) (sizeof(arry) / sizeof(arry[0]))

int thread_num        = 4;
bool use_data_file    = false;
bool Load_Only = false;
std::vector<size_t> scan_size;
std::vector<size_t> sort_scan_size;

using combotree::ComboTree;
using combotree::Random;
using combotree::Timer;
using ycsbc::KvDB;
using namespace dbInter;

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

const uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325;
const uint64_t kFNVPrime64 = 1099511628211;

inline uint64_t FNVHash64(uint64_t val) {
  uint64_t hash = kFNVOffsetBasis64;

  for (int i = 0; i < 8; i++) {
    uint64_t octet = val & 0x00ff;
    val = val >> 8;

    hash = hash ^ octet;
    hash = hash * kFNVPrime64;
  }
  return hash;
}

inline uint64_t Hash(uint64_t val) { return FNVHash64(val); }

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
    "    --use-data-file[-d]      use data file" << std::endl <<
    "    --help[-h]               show help" << std::endl;
}

void Bulk_load_test(ycsbc::KvDB *db, const int distribute);

int main(int argc, char** argv) {
  static struct option opts[] = {
  /* NAME               HAS_ARG            FLAG  SHORTNAME*/
    {"thread",          required_argument, NULL, 't'},
    {"load-size",       required_argument, NULL, 0},
    {"put-size",        required_argument, NULL, 0},
    {"get-size",        required_argument, NULL, 0},
    {"delete-size",     required_argument, NULL, 0},
    {"dbname",       required_argument, NULL, 0},
    {"use-data-file",   no_argument,       NULL, 'd'},
    {"load",            no_argument,       NULL, 'L'},
    {"help",            no_argument,       NULL, 'h'},
    {NULL, 0, NULL, 0}
  };

  int c;
  int opt_idx;
  std::string  dbName= "combotree";
  while ((c = getopt_long(argc, argv, "t:s:dh", opts, &opt_idx)) != -1) {
    switch (c) {
      case 0:
        switch (opt_idx) {
          case 0: thread_num = atoi(optarg); break;
          case 1: LOAD_SIZE = atoi(optarg); break;
          case 2: PUT_SIZE = atoi(optarg); break;
          case 3: GET_SIZE = atoi(optarg); break;
          case 4: DELETE_SIZE = atoi(optarg); break;
          case 5: dbName = optarg; break;
          case 6: use_data_file = true; break;
          case 7: Load_Only = true; break;
          case 8: show_help(argv[0]); return 0;
          default: std::cerr << "Parse Argument Error!" << std::endl; abort();
        }
        break;
      case 't': thread_num = atoi(optarg); break;
      case 's': scan_size.push_back(atoi(optarg)); break;
      case 'd': use_data_file = true; break;
      case 'L': Load_Only = true; break;
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
  } else if(Load_Only) {

  } else {
    Random rnd(0, LOAD_SIZE+PUT_SIZE-1);
    for (size_t i = 0; i < LOAD_SIZE+PUT_SIZE; ++i)
      key.push_back(Hash(i));
    for (size_t i = 0; i < LOAD_SIZE+PUT_SIZE; ++i)
      std::swap(key[i],key[rnd.Next()]);
  }
  NVM::env_init();
  KvDB* db = nullptr;
  if(dbName == "fastfair") {
    db = new FastFairDb();
    // db = new fastfairDB();
  } else if(dbName == "pgm") {
    db = new PGMDynamicDb();
  } else if(dbName == "xindex") {
    db = new XIndexDb();
  } else if(dbName == "alex") {
    db = new AlexDB();
  } else if(dbName == "letree") {
    db = new LetDB();
  } else {
    db = new ComboTreeDb();
  }

  db->Init();
  Timer load_timer;
  Timer get_timer;
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
  uint64_t load_finished = 0;
  uint64_t load_pos = 0, prev_pos = 0;

  Random get_rnd(0, LOAD_SIZE-1);
  load_timer.Record("start");

  if(Load_Only) {
    Bulk_load_test(db, 0);
    goto finished;
  }
  // for(size_t i = 0; i < ArrayLen(load_tests); i ++) {
  //   load_finished = std::min(load_tests[i], LOAD_SIZE);
  //   load_timer.Clear();
  //   load_timer.Record("start");
  //   prev_pos = load_pos;
  //   for(; load_pos < load_finished; load_pos ++) {
  //       auto ret = db->Put(key[load_pos], key[load_pos]);
  //       if (ret != 1) {
  //         std::cout << "load error, key: " << key[load_pos] << ", size: " << load_pos << std::endl;
  //         db->Put(key[load_pos], key[load_pos]);
  //         assert(0);
  //       }

  //       if((load_pos + 1)% PUT_SIZE == 0) {
  //         load_timer.Record("stop");
  //         uint64_t total_time  = load_timer.Microsecond("stop", "start");
  //         std::cout << "[Metic-Write]: After Load "<< prev_pos << " put: " 
  //                 << "cost " << total_time/1000000.0 << "s, " 
  //                 << "iops " << (double)(load_pos - prev_pos + 1)/(double)total_time*1000000.0 
  //                 << std::endl;
  //         load_timer.Clear();
  //         load_timer.Record("start");
  //         prev_pos = load_pos + 1;
  //         { // small get only 10%
  //           size_t value;
  //           get_timer.Clear();
  //           get_timer.Record("start");
  //           for(int i = 0; i < GET_SIZE / 10; i ++) {
  //             bool ret = db->Get(key[get_rnd.Next() % load_pos], value);
  //             if (ret != true) {
  //               std::cout << "get error!" << std::endl;
  //               assert(0);
  //             }
  //           }
  //           get_timer.Record("stop");
  //           uint64_t total_time  = get_timer.Microsecond("stop", "start");
  //           std::cout << "[Metic-Read]: After Load "<< prev_pos << " get: "
  //                   << "cost " << total_time/1000000.0 << "s, " 
  //                   << "iops " << (double)(GET_SIZE / 10)/(double)total_time*1000000.0 
  //                   << std::endl;
  //           db->PrintStatic();
  //         }
  //       }
  //   }
  //   //Get
  //   size_t value;
  //   get_timer.Clear();
  //   get_timer.Record("start");
  //   for(int i = 0; i < GET_SIZE; i ++) {
  //     bool ret = db->Get(key[get_rnd.Next() % load_finished], value);
  //     if (ret != true) {
  //       std::cout << "get error!" << std::endl;
  //       assert(0);
  //     }
  //   }
  //   get_timer.Record("stop");
  //   uint64_t total_time  = get_timer.Microsecond("stop", "start");
  //   std::cout << "[Read]: After Load "<< load_pos << " get: " 
  //           << "cost " << total_time/1000000.0 << "s, "
  //           << "iops " << (double)(GET_SIZE)/(double)total_time*1000000.0 
  //           << std::endl;
  //   if(LOAD_SIZE == load_finished) break;
  // }
  {
    PUT_SIZE = 10000000;
    GET_SIZE = 1000000;
    uint64_t GetMetic = PUT_SIZE;
    for(load_pos = 0; load_pos < LOAD_SIZE; load_pos ++) {
        auto ret = db->Put(key[load_pos], key[load_pos]);
        if (ret != 1) {
          std::cout << "load error, key: " << key[load_pos] << ", size: " << load_pos << std::endl;
          db->Put(key[load_pos], key[load_pos]);
          assert(0);
        }

        if((load_pos + 1)% PUT_SIZE == 0) {
          load_timer.Record("stop");
          uint64_t total_time  = load_timer.Microsecond("stop", "start");
          std::cout << "[Metic-Write]: After Load "<< prev_pos << " put: " 
                  << "cost " << total_time/1000000.0 << "s, " 
                  << "iops " << (double)(load_pos - prev_pos + 1)/(double)total_time*1000000.0 << " .";
          NVM::const_stat.PrintOperate(load_pos - prev_pos + 1);
          std::cout << "[Metic-Write]: "; 
          db->PrintStatic();
          prev_pos = load_pos + 1;
          // if(prev_pos % GetMetic == 0)
          { // small get only 10%
            size_t value;
            get_timer.Clear();
            get_timer.Record("start");
            for(int i = 0; i < GET_SIZE; i ++) {
              bool ret = db->Get(key[get_rnd.Next() % load_pos], value);
              if (ret != true) {
                std::cout << "get error!" << std::endl;
                assert(0);
              }
            }
            get_timer.Record("stop");
            uint64_t total_time  = get_timer.Microsecond("stop", "start");
            std::cout << "[Metic-Read]: After Load "<< prev_pos << " get: "
                    << "cost " << total_time/1000000.0 << "s, " 
                    << "iops " << (double)(GET_SIZE)/(double)total_time*1000000.0 << " .";
            NVM::const_stat.PrintOperate(GET_SIZE);
            std::cout << "[Metic-Read]: ";
            db->PrintStatic();
            // GET_SIZE = pow(10, (int)std::log10(prev_pos) - 1);
            // GET_SIZE = prev_pos / 10;
            // GET_SIZE = std::min(1000000UL, GET_SIZE);
            // GetMetic = std::min(1000000UL, GET_SIZE * 10);
            std::cout << "Get size: " << GET_SIZE << ": " << GetMetic << std::endl;
          }

          load_timer.Clear();
          load_timer.Record("start");
        }
    }
  }
finished:
  delete db;
  NVM::env_exit();
  
  return 0;
}

void Bulk_load_test(ycsbc::KvDB *db, const int distribute) {
  std::vector<uint64_t> keys;
  keys.reserve(LOAD_SIZE + PUT_SIZE);
  Distribute::CaussGenerator rnd(0.0, 2, 1e16);
  std::set<uint64_t> key_set;
  for(int i = 0; i < LOAD_SIZE + PUT_SIZE; i ++) {
    uint64_t key = rnd.Next();
    keys.emplace_back(key);
    key_set.insert(key);
  }
  std::cout << "key total " << keys.size() << ", uniqure " << key_set.size() << std::endl;
  Timer load_timer;
  Timer get_timer;
  uint64_t total_time = 0;
  load_timer.Record("start");
  for(int load_pos = 0; load_pos < LOAD_SIZE; load_pos ++) {
    db->Put(keys[load_pos], keys[load_pos]);
  }
  load_timer.Record("stop");
  total_time  = load_timer.Microsecond("stop", "start");

  std::cout << "[Metic-Load]: Load "<< LOAD_SIZE << ": " 
                  << "cost " << total_time/1000000.0 << "s, " 
                  << "iops " << (double)(LOAD_SIZE)/(double)total_time*1000000.0 
                  << std::endl;
  size_t value;
  Random get_rnd(0, LOAD_SIZE-1);
  get_timer.Record("start");
  for(int i = 0; i < GET_SIZE; i ++) {
    bool ret = db->Get(keys[get_rnd.Next()], value);
    if (ret != true) {
      std::cout << "get error!" << std::endl;
      assert(0);
    }
  }
  get_timer.Record("stop");
  total_time  = get_timer.Microsecond("stop", "start");
  std::cout << "[Metic-Read]: After Load "<< LOAD_SIZE << " get: "
          << "cost " << total_time/1000000.0 << "s, " 
          << "iops " << (double)(GET_SIZE)/(double)total_time*1000000.0 
          << std::endl;

  load_timer.Clear();
  load_timer.Record("start");
  for(int i = 0; i < PUT_SIZE; i ++) {
    db->Put(keys[LOAD_SIZE + i], keys[LOAD_SIZE + i]);
  }

  load_timer.Record("stop");
  total_time  = load_timer.Microsecond("stop", "start");
  std::cout << "[Metic-Write]: After Load "<< LOAD_SIZE << " put: " 
          << "cost " << total_time/1000000.0 << "s, " 
          << "iops " << (double)(PUT_SIZE)/(double)total_time*1000000.0 
          << std::endl;
         
}
