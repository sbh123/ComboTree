#include <iostream>
#include <fstream>
#include <cassert>
#include <iomanip>
#include <sstream>
#include <thread>
#include <getopt.h>
#include <unistd.h>
#include <cmath>
#include <vector>
#include <set>
#include <map>

#include "db_interface.h"
#include "timer.h"
#include "util.h"
#include "random.h"

using combotree::ComboTree;
using combotree::Random;
using combotree::Timer;
using ycsbc::KvDB;
using namespace dbInter;
using namespace util;

struct operation
{
    /* data */
    uint32_t op_type;
    uint32_t op_len; // for only scan
    uint64_t key_num;
};

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
int Loads_type = 0;

int main(int argc, char *argv[]) {
    static struct option opts[] = {
  /* NAME               HAS_ARG            FLAG  SHORTNAME*/
    {"thread",          required_argument, NULL, 't'},
    {"load-size",       required_argument, NULL, 0},
    {"put-size",        required_argument, NULL, 0},
    {"get-size",        required_argument, NULL, 0},
    {"dbname",          required_argument, NULL, 0},
    {"workload",        required_argument, NULL, 0},
    {"loadstype",       required_argument, NULL, 0},
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
          case 6: Loads_type = atoi(optarg); break;
          case 7: show_help(argv[0]); return 0;
          default: std::cerr << "Parse Argument Error!" << std::endl; abort();
        }
        break;
      case 't': thread_num = atoi(optarg); break;
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

  std::vector<uint64_t> data_base;
  switch (Loads_type)
  {
  case -2:
    data_base = read_data_from_osm<uint64_t>(load_file, get_cellid);
    break;
  case -1:
    data_base = read_data_from_osm<uint64_t>(load_file, get_longlat);
    break;
  case 0:
    data_base = generate_uniform_random(LOAD_SIZE + PUT_SIZE * 8);
    break;
  case 1:
    data_base = generate_random_ycsb(LOAD_SIZE + PUT_SIZE * 8);
    break;
  case 2:
    data_base = load_data_from_osm<uint64_t>("/home/sbh/generate_random_osm_cellid.dat");;
    break;
  case 3:
    data_base = load_data_from_osm<uint64_t>("/home/sbh/generate_random_osm_longlat.dat");;
    break;
  default:
    data_base = generate_uniform_random(LOAD_SIZE + PUT_SIZE * 8);
    break;
  }

  NVM::env_init();
  KvDB* db = nullptr;
  if(dbName == "fastfair") {
    db = new fastfairDB();
  } else if(dbName == "xindex") {
    db = new XIndexDb(2, thread_num);
  } else if(dbName == "letree") {
    db = new LetDB();
  } else {
    db = new LetDB();
  }
  db->Init();
  Timer timer;
  uint64_t us_times;
  uint64_t load_pos = 0; 
  std::cout << "Start run ...." << std::endl;
  // {
  //   int init_size = LOAD_SIZE;
  //   std::mt19937_64 gen_payload(std::random_device{}());
  //   auto values = new std::pair<uint64_t, uint64_t>[init_size];
  //   for (int i = 0; i < init_size; i++) {
  //     // values[i].first = data_base[data_base.size() - i - 1];
  //     values[i].first = data_base[i];
  //     values[i].second = static_cast<uint64_t>(gen_payload());
  //   }
  //   std::sort(values, values + init_size,
  //             [](auto const& a, auto const& b) { return a.first < b.first; });
  //   db->Bulk_load(values, init_size);
  // }

  {
     // Load
    timer.Record("start");
    std::vector<std::thread> threads;
    std::atomic_int thread_id_count(0);
    size_t per_thread_size = LOAD_SIZE / thread_num;
    for(int i = 0; i < thread_num; i ++) {
        threads.emplace_back([&](){
            int thread_id = thread_id_count.fetch_add(1);
            size_t start_pos = thread_id * per_thread_size;
            size_t size = (thread_id == thread_num-1) ? LOAD_SIZE-(thread_num-1)*per_thread_size : per_thread_size;
            for (size_t j = 0; j < size; ++j) {
                auto ret = db->MultPut(data_base[start_pos+j], data_base[start_pos+j], thread_id);
                if (ret != 1) {
                    std::cout << "load error, key: " << data_base[start_pos+j] << ", size: " << j << std::endl;
                    db->MultPut(data_base[start_pos+j], data_base[start_pos+j], thread_id);
                    assert(0);
                }

                if(thread_id == 0 && (j + 1) % 100000 == 0) std::cerr << "Operate: " << j + 1 << '\r';  
            }
        });
    }
    for (auto& t : threads)
        t.join();

    timer.Record("stop");
    us_times = timer.Microsecond("stop", "start");
    load_pos = LOAD_SIZE;
    std::cout << "[Metic-Load]: Load " << LOAD_SIZE << ": " 
              << "cost " << us_times/1000000.0 << "s, " 
              << "iops " << (double)(LOAD_SIZE)/(double)us_times*1000000.0 << " ." << std::endl;
  }

  {
     // Put
    std::vector<std::thread> threads;
    std::atomic_int thread_id_count(0);
    size_t per_thread_size = PUT_SIZE / thread_num;
    timer.Clear();
    timer.Record("start");
    for(int i = 0; i < thread_num; i ++) {
        threads.emplace_back([&](){
            int thread_id = thread_id_count.fetch_add(1);
            size_t start_pos = thread_id * per_thread_size + LOAD_SIZE;
            size_t size = (thread_id == thread_num-1) ? PUT_SIZE-(thread_num-1)*per_thread_size : per_thread_size;
            for (size_t j = 0; j < size; ++j) {
                auto ret = db->MultPut(data_base[start_pos+j], data_base[start_pos+j], thread_id);
                if (ret != 1) {
                    std::cout << "Put error, key: " << data_base[start_pos+j] << ", size: " << j << std::endl;
                    db->MultPut(data_base[start_pos+j], data_base[start_pos+j], thread_id);
                    assert(0);
                }
                if(thread_id == 0 && (j + 1) % 100000 == 0) std::cerr << "Operate: " << j + 1 << '\r'; 
            }
        });
    }
    for (auto& t : threads)
        t.join();
        
    timer.Record("stop");
    us_times = timer.Microsecond("stop", "start");
    std::cout << "[Metic-Put]: Put " << PUT_SIZE << ": " 
              << "cost " << us_times/1000000.0 << "s, " 
              << "iops " << (double)(PUT_SIZE)/(double)us_times*1000000.0 << " ." << std::endl;
  }
  {
     // Get
    std::vector<std::thread> threads;
    std::atomic_int thread_id_count(0);
    size_t per_thread_size = GET_SIZE / thread_num;
    Random get_rnd(0, GET_SIZE-1);
    for (size_t i = 0; i < GET_SIZE; ++i)
        std::swap(data_base[i],data_base[get_rnd.Next()]);
    timer.Clear();
    timer.Record("start");
    for (int i = 0; i < thread_num; ++i) {
        threads.emplace_back([&](){
            int thread_id = thread_id_count.fetch_add(1);
            size_t start_pos = thread_id *per_thread_size;
            size_t size = (thread_id == thread_num-1) ? GET_SIZE-(thread_num-1)*per_thread_size : per_thread_size;
            size_t value;
            for (size_t j = 0; j < size; ++j) {
                bool ret = db->MultGet(data_base[start_pos+j], value, thread_id);
                if (ret != true) {
                    std::cout << "Get error!" << std::endl;
                    assert(0);
                }
                if(thread_id == 0 && (j + 1) % 100000 == 0) std::cerr << "Operate: " << j + 1 << '\r'; 
            }
        });
    }
    for (auto& t : threads)
        t.join();
        
    timer.Record("stop");
    us_times = timer.Microsecond("stop", "start");
    std::cout << "[Metic-Get]: Get " << GET_SIZE << ": " 
              << "cost " << us_times/1000000.0 << "s, " 
              << "iops " << (double)(GET_SIZE)/(double)us_times*1000000.0 << " ." << std::endl;
  }

  delete db;

  NVM::env_exit();
  return 0;
}