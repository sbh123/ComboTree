#include <iostream>
#include <fstream>
#include <cassert>
#include <iomanip>
#include <sstream>
#include <vector>
#include <chrono>
#include <algorithm>


#include "learnindex/pgm_index.hpp"
#include "learnindex/rmi_impl.h"
#include "learnindex/learn_index.h"

#include "random.h"

using combotree::Random;
using pgm::PGMIndex;
using pgm::ApproxPos;
using RMI::Key_64;
using RMI::TwoStageRMI;
using LI::LearnIndex;
typedef RMI::Key_64 rmi_key_t;

static ApproxPos near_search_key(const std::vector<uint64_t> &keys, const uint64_t &key, size_t pos) {
    long lo, hi;
    if(keys[pos] < key) {
        lo = pos;
        hi = pos + 16;
        while(hi < keys.size() && keys[hi] < key) { lo = hi; hi += 16; }
        if(hi > keys.size()) hi = keys.size();
    } else {
        hi = pos;
        lo = pos - 16;
        while(lo > 0 && keys[lo] > key) { hi = lo; lo -= 16; }
        if(lo < 0) lo = 0;
    }
    return {(lo + hi) / 2, lo, hi};
}

int main() {
    Random rnd(0, UINT64_MAX - 1);
    {
        int size = 1000000;
        const int epsilon = 8; // space-time trade-off parameter
        PGMIndex<uint64_t, epsilon> *pgm_index = nullptr;
        TwoStageRMI<Key_64> *rmi_index = nullptr;
        LearnIndex *learn_index = nullptr;
        std::vector<uint64_t> pgm_keys;
        std::vector<Key_64> rmi_keys;
        for(int i = 0; i < size; i++) {
            uint64_t key = rnd.Next();
            pgm_keys.push_back(key);
        }
        std::sort(pgm_keys.begin(), pgm_keys.end());
        for(int i = 0; i < size; i++) {
            rmi_keys.push_back(Key_64(pgm_keys[i]));
        }

        {
            auto startTime = std::chrono::system_clock::now();
            pgm_index = new PGMIndex<uint64_t, epsilon>(pgm_keys);
            auto endTime = std::chrono::system_clock::now();
            std::cout << "[PGM]: PGM Index train cost: " 
                << std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime).count()
                << " ms" << std::endl;
            std::cout << "[PGM]: Segments count " << pgm_index->segments_count()
                << ", Height " << pgm_index->height() << std::endl;
        }
        {
            auto startTime = std::chrono::system_clock::now();
            learn_index = new LearnIndex(pgm_keys);
            auto endTime = std::chrono::system_clock::now();
            std::cout << "[Learn-Index]: Learn-Index train cost: " 
                << std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime).count()
                << " ms" << std::endl;
            // std::cout << "[Learn-Index]: Segments count " << pgm_index_bottom_segmet.segments_count()
            //     << ", Height " << pgm_index_bottom_segmet.height() << std::endl;
        }
        {
            auto startTime = std::chrono::system_clock::now();
            rmi_index = new TwoStageRMI<Key_64>(rmi_keys);
            auto endTime = std::chrono::system_clock::now();
            std::cout << "[RMI]: RMI Index train cost: " 
                << std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime).count()
                << " ms" << std::endl;
            std::cout << "[RMI]: Two stage model count " << rmi_index->rmi_model_n()
                << std::endl;
        }
        std::vector<uint64_t> pgmDurations;
        std::vector<uint64_t> rmiDurations;
        std::vector<uint64_t> learnDurations;
        for(int i = 0; i < pgm_keys.size(); i += 10) {
            auto startTime = std::chrono::system_clock::now();
            auto range1 = pgm_index->search(pgm_keys[i]);
            // auto range2 = learn_index.search(pgm_keys[i]);
            auto endTime = std::chrono::system_clock::now();
            uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
            pgmDurations.push_back(ns);
            // if(!(range1 == range2)) {
            //     // auto range2 = learn_index.search(pgm_keys[i], true);
            //     std::cout << "Range 1: " << range1.lo << " : " << range1.hi << " : " << range1.pos << std::endl;
            //     std::cout << "Range 2: " << range2.lo << " : " << range2.hi << " : " << range2.pos << std::endl;
            //     auto range2 = learn_index.search(pgm_keys[i], true);
            // }
        }
        for(int i = 0; i < pgm_keys.size(); i += 10) {
            auto startTime = std::chrono::system_clock::now();
            int predict_pos = rmi_index->predict(rmi_keys[i]);
            auto range = near_search_key(pgm_keys, pgm_keys[i], predict_pos);
            auto endTime = std::chrono::system_clock::now();
            uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
            rmiDurations.push_back(ns);
            assert(abs(predict_pos - i) < 100);
            
        }

        for(int i = 0; i < pgm_keys.size(); i += 10) {
            auto startTime = std::chrono::system_clock::now();
            auto range = learn_index->search(pgm_keys[i]);
            auto endTime = std::chrono::system_clock::now();
            uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
            learnDurations.push_back(ns);
        }

        auto summaryStats = [](const std::vector<uint64_t> &durations, const char *name = "PGM") {
            double average = std::accumulate(durations.cbegin(), durations.cend() - 1, 0.0) / durations.size();
            auto minmax = std::minmax(durations.cbegin(), durations.cend() - 1);

            std::cout << "[" << name << "]: " << "Min: " << *minmax.first << std::endl;
            std::cout << "[" << name << "]: " << "Average: " << average << std::endl;
            std::cout << "[" << name << "]: " << "Max: " << *minmax.second << std::endl;
        };
        summaryStats(pgmDurations, "PGM");
        summaryStats(rmiDurations, "RMI");
        summaryStats(learnDurations, "Learn-Index");

        delete pgm_index;
        delete rmi_index;
        delete learn_index;
    }
    return 0;
}