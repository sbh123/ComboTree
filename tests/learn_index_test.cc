#include <iostream>
#include <fstream>
#include <cassert>
#include <iomanip>
#include <sstream>
#include <vector>
#include <chrono>
#include <algorithm>

#define USE_STD_ITER

#include "nvm_alloc.h"
#include "learnindex/rmi_impl.h"
#include "learnindex/learn_index.h"
#include "fast-fair/btree.h"
#include "random.h"

using combotree::Random;
// using pgm::PGMIndex;
// using pgm::ApproxPos;
using PGM_NVM::PGMIndex;
using PGM_NVM::ApproxPos;
using RMI::Key_64;
using RMI::TwoStageRMI;
using LI::LearnIndex;
using FastFair::btree;

typedef RMI::Key_64 rmi_key_t;

static ApproxPos near_search_key(const uint64_t *keys, const uint64_t &key, size_t pos, size_t size) {
    long lo, hi;
    if(keys[pos] < key) {
        lo = pos;
        hi = pos + 16;
        while(hi < size && keys[hi] < key) { lo = hi; hi += 16; }
        if(hi > size) hi = size;
    } else {
        hi = pos;
        lo = pos - 16;
        while(lo > 0 && keys[lo] > key) { hi = lo; lo -= 16; }
        if(lo < 0) lo = 0;
    }
    return {(lo + hi) / 2, lo, hi};
}

int main(int argc, char *argv[]) {
    size_t size = 100000;
    if(argc > 1) {
        size = atoi(argv[1]);
    }
    NVM::env_init();
    Random rnd(0, UINT64_MAX - 1);
    {
        const int epsilon = 8; // space-time trade-off parameter
        PGMIndex<uint64_t, epsilon> *pgm_index = nullptr;
        TwoStageRMI<Key_64> *rmi_index = nullptr;
        LearnIndex *learn_index = nullptr;
        btree *btree = new FastFair::btree();
        // std::vector<uint64_t> pgm_keys;
        uint64_t *pgm_keys = (uint64_t *)NVM::data_alloc->alloc(size * sizeof(uint64_t));
        for(size_t i = 0; i < size; i++) {
            uint64_t key = rnd.Next();
            pgm_keys[i] = (key);
            btree->btree_insert(key, (char *)key);
        }

        // std::sort(pgm_keys, pgm_keys + size);
        btree->btree_search_range(0, UINT64_MAX, pgm_keys);

        {
            auto startTime = std::chrono::system_clock::now();
            pgm_index = new PGMIndex<uint64_t, epsilon>(pgm_keys, pgm_keys + size);
            auto endTime = std::chrono::system_clock::now();
            std::cout << "[PGM]: PGM Index train cost: " 
                << std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime).count() / 1e6
                << " s" << std::endl;
            std::cout << "[PGM]: Segments count " << pgm_index->segments_count()
                << ", Height " << pgm_index->height() << std::endl;
        }
        {
            auto startTime = std::chrono::system_clock::now();
            rmi_index = new TwoStageRMI<Key_64>(pgm_keys, pgm_keys + size);
            auto endTime = std::chrono::system_clock::now();
            std::cout << "[RMI]: RMI Index train cost: " 
                << std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime).count() / 1e6
                << " ms" << std::endl;
            std::cout << "[RMI]: Two stage model count " << rmi_index->rmi_model_n()
                << std::endl;
        }

        {
            auto startTime = std::chrono::system_clock::now();
            learn_index = new LearnIndex(pgm_keys, pgm_keys + size);
            auto endTime = std::chrono::system_clock::now();
            std::cout << "[Learn-Index]: Learn-Index train cost: " 
                << std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime).count() / 1e6
                << " s" << std::endl;
            // std::cout << "[Learn-Index]: Segments count " << pgm_index_bottom_segmet.segments_count()
            //     << ", Height " << pgm_index_bottom_segmet.height() << std::endl;
        }
        std::vector<double> pgmDurations;
        std::vector<double> rmiDurations;
        std::vector<double> learnDurations;
        std::vector<double> btreeDurations;
        for(size_t i = 0; i < size; i += 10) {
            int idx  = rnd.Next() % size;
            auto startTime = std::chrono::system_clock::now();
            auto range = pgm_index->search(pgm_keys[idx]);
            int pos = std::lower_bound(pgm_keys + range.lo,  pgm_keys + range.hi, pgm_keys[idx]) - pgm_keys;
            auto endTime = std::chrono::system_clock::now();
            uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
            pgmDurations.push_back(1.0 * ns);
            assert(pos == idx);
        }
        std::cout << "[PGM]: PGM search finished. " << std::endl;

        for(size_t i = 0; i < size; i += 10) {
            int idx  = rnd.Next() % size;
            auto startTime = std::chrono::system_clock::now();
            int predict_pos = rmi_index->predict(rmi_key_t(pgm_keys[idx]));
            auto range = near_search_key(pgm_keys, pgm_keys[idx], predict_pos, size);
            int pos = std::lower_bound(pgm_keys + range.lo,  pgm_keys + range.hi, pgm_keys[idx]) - pgm_keys;
            auto endTime = std::chrono::system_clock::now();
            uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
            rmiDurations.push_back(1.0 * ns);
            if(pos != idx) {
                std::cout << "Failed i = " << idx << std::endl;
                std::cout << "Lower: " << range.lo << ", higher: " <<  range.hi << std::endl;
            }
            assert(pos == idx);
        }

        std::cout << "[RMI]: RMI search finished. " << std::endl;

        for(size_t i = 0; i < size; i += 10) {
            int idx  = rnd.Next() % size;
            auto startTime = std::chrono::system_clock::now();
            auto range = learn_index->search(pgm_keys[idx]);
            int pos = std::lower_bound(pgm_keys + range.lo,  pgm_keys + range.hi, pgm_keys[idx]) - pgm_keys;
            auto endTime = std::chrono::system_clock::now();
            uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
            learnDurations.push_back(1.0 * ns);
            assert(pos == idx);
        }

        std::cout << "[LI]: Learn-Index search finished. " << std::endl;

        for(size_t i = 0; i < size; i += 10) {
            int idx  = rnd.Next() % size;
            auto startTime = std::chrono::system_clock::now();
            // char *pvalue = btree->btree_search(pgm_keys[idx]);
            btree->btree_search_leaf(pgm_keys[idx]);
            auto endTime = std::chrono::system_clock::now();
            uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
            btreeDurations.push_back(1.0 * ns);
            // assert(pvalue == (char *)pgm_keys[idx]);
        }

        std::cout << "[Fast-Fair]: Fast-Fair search finished. " << std::endl;

        auto summaryStats = [](const std::vector<double> &durations, const char *name = "PGM") {
            double average = std::accumulate(durations.cbegin(), durations.cend() - 1, 0.0) / durations.size();
            auto minmax = std::minmax(durations.cbegin(), durations.cend() - 1);

            std::cout << "[" << name << "]: " << "Min: " << *minmax.first << std::endl;
            std::cout << "[" << name << "]: " << "Average: " << average << std::endl;
            std::cout << "[" << name << "]: " << "Max: " << *minmax.second << std::endl;
        };
        summaryStats(pgmDurations, "PGM");
        summaryStats(rmiDurations, "RMI");
        summaryStats(learnDurations, "LI");
        summaryStats(btreeDurations, "Fast-Fair");
        
        delete learn_index;
        delete rmi_index;
        delete pgm_index;
        delete btree;
        NVM::data_alloc->Free(pgm_keys, size * sizeof(uint64_t));
    }
    NVM::env_exit();
    return 0;
}