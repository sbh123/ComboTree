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

#include "learnindex/pgm_index_nvm.hpp"
#include "learnindex/pgm_index_dynamic.hpp"

#include "../src/combotree_config.h"
#include "nvm_alloc.h"
#include "random.h"


using combotree::Random;
using PGM_NVM::PGMIndex;
using NVM::Alloc;

void pgm_dynamic_test();
int main() {
    Random rnd(0, UINT64_MAX - 1);
    NVM::env_init();
    {
        // int size = 10;
        // uint64_t *index_data = (uint64_t *)malloc(size * sizeof(uint64_t));
        // index_data[0] = 6; index_data[1] = 9, index_data[2] = 11;
        // index_data[3] = 13; index_data[4] = 14, index_data[5] = 15;
        // index_data[6] = 18; index_data[7] = 27, index_data[8] = 37;
        // index_data[9] = 45; 
        // // uint64_t key = index_data[size / 2];
        // const int epsilon = 2; // space-time trade-off parameter
        // PGMIndex<uint64_t, epsilon> index(index_data, index_data + size);
        // free(index_data);
    }
    {
        int size = 10000;
        uint64_t *index_data = (uint64_t *)malloc(size * sizeof(uint64_t));
        for(int i = 0; i < size; i++) {
            index_data[i] = rnd.Next();
        }
        std::sort(index_data, index_data + size);
        const int epsilon = 16; // space-time trade-off parameter
        PGMIndex<uint64_t, epsilon> index(index_data, index_data + size);
        std::cout << "Index segmet size is : " << index.segments_count() << std::endl;
        std::vector<double> pgmDurations;
        for(int i = 0; i < size; i ++) {
            auto startTime = std::chrono::system_clock::now();
            auto range = index.search(index_data[i]);
            int pos = std::lower_bound(index_data + range.lo,  index_data + range.hi, index_data[i]) - index_data;
            auto endTime = std::chrono::system_clock::now();
            uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
            pgmDurations.push_back(1.0 * ns);
            assert(pos == i);
        }
        auto summaryStats = [](const std::vector<double> &durations, const char *name = "PGM") {
            double average = std::accumulate(durations.cbegin(), durations.cend() - 1, 0.0) / durations.size();
            auto minmax = std::minmax(durations.cbegin(), durations.cend() - 1);

            std::cout << "[" << name << "]: " << "Min: " << *minmax.first << std::endl;
            std::cout << "[" << name << "]: " << "Average: " << average << std::endl;
            std::cout << "[" << name << "]: " << "Max: " << *minmax.second << std::endl;
        };
        summaryStats(pgmDurations, "PGM");
        free(index_data);
    }
    pgm_dynamic_test();
    NVM::env_exit();
    return 0;
}                       

void pgm_dynamic_test()
{
    typedef uint64_t Key_t;
    typedef char * Value_t;

    using PGMType = PGM_OLD_NVM::PGMIndex<uint64_t>;
    typedef pgm::DynamicPGMIndex<Key_t, Value_t, PGMType> db_t;
    Random rnd(0, UINT64_MAX - 1);
    db_t *db = new db_t();
    std::cout<< "DB: " << db << std::endl;
    int test_num =100;
    std::vector<uint64_t> keys;
    for(int i = 0; i < test_num; i ++) {
        uint64_t key = rnd.Next();
        db->insert(key, (char *)key);
        keys.push_back(key);
    }
    for(int i = 0; i < test_num; i ++) {
        auto it = db->find(keys[i]);
        assert(it->first == (uint64_t)(it->second));
    }
    std::distance(db->begin(), db->end());
    for(auto it = db->begin(); it != db->end(); ++ it) {
        // std::cout << "Key: " << it->first << std::endl;
    }
    for(int i = 0; i < 10; i ++) {
        db->erase(keys[i]);
    }
    delete db;
    std::vector<int> intvec;
    intvec.insert(intvec.begin(), 0);
}