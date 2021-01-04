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

#include "../src/combotree_config.h"
#include "nvm_alloc.h"
#include "random.h"


using combotree::Random;
using PGM_NVM::PGMIndex;
using NVM::Alloc;

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
        int size = 10000000;
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
    NVM::env_exit();
    return 0;
}                        