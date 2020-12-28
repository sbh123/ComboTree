#pragma once

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

namespace LI
{
using pgm::PGMIndex;
using pgm::ApproxPos;
using RMI::Key_64;
using RMI::TwoStageRMI;
typedef RMI::Key_64 rmi_key_t;

class LearnIndex {
static const size_t epsilon = 8;
public:
    // LearnIndex(const std::vector<uint64_t> &keys) {
    //     pgm_index_ = new PGMIndex<uint64_t, epsilon>(keys.begin(), keys.end(), true);
    //     std::vector<rmi_key_t> rmi_keys;
    //     for(size_t i = 0; i < pgm_index_->segments_count(); i ++) {
    //         uint64_t key = pgm_index_->segments[i].key;
    //         rmi_keys.push_back(rmi_key_t(key));
    //     }
    //     rmi_index_ = new TwoStageRMI<rmi_key_t, 4>(rmi_keys);
    // }

    template<typename RandomIt>
    LearnIndex(RandomIt key_start, RandomIt key_end) {
        pgm_index_ = new PGMIndex<uint64_t, epsilon>(key_start, key_end, true);
        std::vector<rmi_key_t> rmi_keys;
        for(size_t i = 0; i < pgm_index_->segments_count(); i ++) {
            uint64_t key = pgm_index_->segments[i].key;
            rmi_keys.push_back(rmi_key_t(key));
        }
        rmi_index_ = new TwoStageRMI<rmi_key_t, 4>(rmi_keys);
    }
    ~LearnIndex() {
        if(pgm_index_) delete pgm_index_;
        if(rmi_index_) delete rmi_index_;
    }
    ApproxPos search(const uint64_t &key, bool debug = false) {
        size_t predict_pos = rmi_index_->predict(rmi_key_t(key));
        if(debug)
        std::cout << "Predict position: " << predict_pos << std::endl;
        return pgm_index_->search_near_pos(key, predict_pos, debug);
    }
    size_t segments_count() { return pgm_index_->segments_count();}
private:
    PGMIndex<uint64_t, epsilon> *pgm_index_;
    // std::vector<pgm::PGMIndex<uint64_t, epsilon>::Segment> segments;
    TwoStageRMI<rmi_key_t, 4> *rmi_index_;
};

} // namespace LearnIndex