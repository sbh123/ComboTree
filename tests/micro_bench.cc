#include <iostream>
#include <fstream>
#include <cassert>
#include <iomanip>
#include <sstream>
#include <thread>
#include <getopt.h>
#include <unistd.h>
#include <cmath>

#include "db_interface.h"
#include "timer.h"
#include "util.h"

using combotree::ComboTree;
// using combotree::Random;
using combotree::Timer;
using ycsbc::KvDB;
using namespace dbInter;

struct operation
{
    /* data */
    uint32_t op_type;
    uint32_t op_len; // for only scan
    uint64_t key_num;
};

std::vector<uint32_t> generate_random_operation(uint32_t data_size, uint32_t op_num)
{
    std::vector<uint32_t> data; 
    util::FastRandom ranny(42);
    size_t num_generated = 0;
    data.resize(op_num);
    while (num_generated < op_num)
    {
        /* code */
        data[num_generated] = ranny.RandUint32(0, data_size - 1);
        num_generated ++;
    }
    return data;
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
    "    --workload               WorkLoad" << std::endl <<
    "    --help[-h]               show help" << std::endl;
}

int thread_num = 1;
size_t LOAD_SIZE   = 10000000;
size_t PUT_SIZE    = 6000000;
size_t GET_SIZE    = 1000000;
size_t DELETE_SIZE = 1000000;
bool Load_Only = false;

int main(int argc, char *argv[]) {
    static struct option opts[] = {
  /* NAME               HAS_ARG            FLAG  SHORTNAME*/
    {"thread",          required_argument, NULL, 't'},
    {"load-size",       required_argument, NULL, 0},
    {"put-size",        required_argument, NULL, 0},
    {"get-size",        required_argument, NULL, 0},
    {"dbname",          required_argument, NULL, 0},
    {"workload",        required_argument, NULL, 0},
    {"load",            no_argument,       NULL, 'L'},
    {"help",            no_argument,       NULL, 'h'},
    {NULL, 0, NULL, 0}
  };

  int c;
  int opt_idx;
  std::string  dbName= "combotree";
  std::string  load_file= "";
  while ((c = getopt_long(argc, argv, "t:s:dh", opts, &opt_idx)) != -1) {
    switch (c) {
      case 0:
        switch (opt_idx) {
          case 0: thread_num = atoi(optarg); break;
          case 1: LOAD_SIZE = atoi(optarg); break;
          case 2: PUT_SIZE = atoi(optarg); break;
          case 3: GET_SIZE = atoi(optarg); break;
          case 4: dbName = optarg; break;
          case 5: load_file = optarg; break;
          case 7: Load_Only = true; break;
          case 8: show_help(argv[0]); return 0;
          default: std::cerr << "Parse Argument Error!" << std::endl; abort();
        }
        break;
      case 't': thread_num = atoi(optarg); break;
      case 'L': Load_Only = true; break;
      case 'h': show_help(argv[0]); return 0;
      case '?': break;
      default:  std::cout << (char)c << std::endl; abort();
    }
  }

  std::cout << "THREAD NUMBER:         " << thread_num << std::endl;
  std::cout << "LOAD_SIZE:             " << LOAD_SIZE << std::endl;
  std::cout << "PUT_SIZE:              " << PUT_SIZE << std::endl;
  std::cout << "GET_SIZE:              " << GET_SIZE << std::endl;
  std::cout << "DB  name:              " << dbName << std::endl;
  std::cout << "Workload:              " << load_file << std::endl;

  const DataType type = util::resolve_type(load_file);

  assert(type == DataType::UINT64);

  const std::vector<uint64_t> data_base = util::load_data<uint64_t>(load_file, 1e6);

  // Load 

  NVM::env_init();
  KvDB* db = nullptr;
  if(dbName == "fastfair") {
    db = new FastFairDb();
  } else if(dbName == "pgm") {
    db = new PGMDynamicDb();
  } else if(dbName == "xindex") {
    db = new XIndexDb();
  } else if(dbName == "alex") {
    db = new AlexDB();
  } else if(dbName == "stx") {
    db = new StxDB();
  } else {
    db = new ComboTreeDb();
  }
  db->Init();
   // Load
  const std::vector<uint32_t> op_seqs = generate_random_operation(data_base.size(), LOAD_SIZE + PUT_SIZE);
  Timer timer;
  uint64_t us_times;
  uint64_t load_pos = 0; 
  {
    timer.Record("start");
    for(load_pos = 0; load_pos < LOAD_SIZE; load_pos ++) {
      db->Put(data_base[op_seqs[load_pos]], data_base[op_seqs[load_pos]]);
    }
    timer.Record("stop");
    us_times = timer.Microsecond("stop", "start");
    std::cout << "[Metic-Load]: Load " << LOAD_SIZE << ": " 
              << "cost " << us_times/1000000.0 << "s, " 
              << "iops " << (double)(LOAD_SIZE)/(double)us_times*1000000.0 << " ." << std::endl;

  }
  
  // us_times = timer.Microsecond("stop", "start");
  // timer.Record("start");
  // Different insert_ration
  float insert_ratio = 0;
  util::FastRandom ranny(18);
  {
    uint64_t value = 0;
    timer.Record("start");
    for(uint64_t i = 0; i < GET_SIZE; i ++) {
      if(ranny.ScaleFactor() < insert_ratio) {
        db->Put(data_base[op_seqs[load_pos]], data_base[op_seqs[load_pos]]);
        load_pos ++;
      } else {
        uint32_t op_seq = op_seqs[ranny.RandUint32(0, load_pos - 1)];
        db->Get(data_base[op_seqs[load_pos]], value);
      }
    }
    timer.Record("stop");
    us_times = timer.Microsecond("stop", "start");
    std::cout << "[Metic-Operate]: Operate " << GET_SIZE << " insert_ratio "<< insert_ratio <<  ": " 
              << "cost " << us_times/1000000.0 << "s, " 
              << "iops " << (double)(LOAD_SIZE)/(double)us_times*1000000.0 << " ." << std::endl;
  }

  delete db;

  return 0;
}