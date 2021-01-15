#include <iostream>
#include <fstream>
#include <cassert>
#include <iomanip>
#include <sstream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <libpmem.h>

#define USE_STD_ITER

#include "nvm_alloc.h"
#include "random.h"
#include "learnindex/rmi.h"
#include "xindex/xindex.h"
#include "xindex/xindex_impl.h"

typedef RMI::Key_64 index_key_t;
typedef xindex::XIndex<index_key_t, uint64_t> xindex_t;


inline void 
prepare_xindex(xindex_t *&table, size_t table_size, int fg_n, int bg_n);

int main() {

    combotree::Random rnd(0, UINT64_MAX - 1);
    NVM::env_init();
    NVM::data_init();
    {
        xindex_t *xindex = nullptr;
        int size = 100000;
        prepare_xindex(xindex, 10000, 1, 1);
        
        std::vector<double> xindexDurations;
        for(int i = 0; i < size; i ++) {
            uint64_t key = rnd.Next();
            auto startTime = std::chrono::system_clock::now();
            xindex->put(index_key_t(key), key, 0);
            auto endTime = std::chrono::system_clock::now();
            uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
            xindexDurations.push_back(1.0 * ns);
        }
        auto summaryStats = [](const std::vector<double> &durations, const char *name = "PGM") {
            double average = std::accumulate(durations.cbegin(), durations.cend() - 1, 0.0) / durations.size();
            auto minmax = std::minmax(durations.cbegin(), durations.cend() - 1);

            std::cout << "[" << name << "]: " << "Min: " << *minmax.first << std::endl;
            std::cout << "[" << name << "]: " << "Average: " << average << std::endl;
            std::cout << "[" << name << "]: " << "Max: " << *minmax.second << std::endl;
        };
        summaryStats(xindexDurations, "XIndex");
        delete xindex;

    }
    {
        struct Int64 {
            int64_t key;
        };
        struct NvmInt64_t: public Int64, public NVM::NvmStructBase {
            uint64_t key;
            NvmInt64_t() {
            }
        };
        std::cout << "NVM Int64 size: " << sizeof(NvmInt64_t) << std::endl;

        NvmInt64_t *key = new NvmInt64_t;
        key->key = 12345678987654321;
        key->Int64::key = 12345678987654321;
        NvmInt64_t *keys = new NvmInt64_t[10]();
        std::cout <<"Key addrs: " << key << std::endl;
        std::cout <<"Key: " << key->key << ", Int64: " << key->Int64::key << std::endl;
        for(int i = 0; i < 11; i ++) {
            key = keys + i;
            std::cout <<"[" << i << "]Key: " << key->key << ", Int64: " << key->Int64::key << std::endl;
        }
        std::cout <<"Keys addrs: " << keys << std::endl;

        // char buf = new char[8];
        delete key;
        delete keys;
    }
    NVM::env_exit();
    return 0;
}                       

inline void 
prepare_xindex(xindex_t *&table, size_t table_size, int fg_n, int bg_n) {
  // prepare data
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int64_t> rand_int64(
      0, std::numeric_limits<int64_t>::max());
  std::vector<index_key_t> initial_keys;
  initial_keys.reserve(table_size);
  for (size_t i = 0; i < table_size; ++i) {
    initial_keys.push_back(index_key_t(rand_int64(gen)));
  }
  // initilize XIndex (sort keys first)
  std::sort(initial_keys.begin(), initial_keys.end());
  std::vector<uint64_t> vals(initial_keys.size(), 1);
  table = new xindex_t(initial_keys, vals, fg_n, bg_n);
}