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
#include "learnindex/rmi_impl.h"
#include "../src/combotree_config.h"
#include "../src/rmi_model.h"
#include "nvm_alloc.h"
#include "random.h"
#include "../src/letree.h"


using combotree::Random;
using PGM_NVM::PGMIndex;
using NVM::Alloc;

void pgm_dynamic_test();
void rmi_model_test();
void letree_test();
int main() {
    Random rnd(0, UINT64_MAX - 1);
    NVM::env_init();
    // {
    //     int size = 20;
    //     uint64_t *index_data = (uint64_t *)malloc(size * sizeof(uint64_t));
    //     // index_data[0] = 6; index_data[1] = 9, index_data[2] = 11;
    //     // index_data[3] = 13; index_data[4] = 14, index_data[5] = 15;
    //     // index_data[6] = 18; index_data[7] = 27, index_data[8] = 37;
    //     // index_data[9] = 45; 
    //     for(int i = 0; i < size; i++) index_data[i] = rnd.Next() % 50;
    //     std::sort(index_data, index_data + size);
    //     // uint64_t key = index_data[size / 2];
    //     const int epsilon = 1; // space-time trade-off parameter
    //     index_data[size-1]=47;
    //     PGMIndex<uint64_t, epsilon> index(index_data, index_data + size);
    //     for(int i = 0; i < size; i++) std::cout << i << " " << index_data[i] << std::endl;

    //     RMI::LinearModel<RMI::Key_64> LM;
    //     LM.prepare_model(index_data, 0, 20);
    //     free(index_data);
    //     return 0;
    // }
    // NVM::data_init();
    // {
    //     int size = 10000;
    //     uint64_t *index_data = (uint64_t *)malloc(size * sizeof(uint64_t));
    //     for(int i = 0; i < size; i++) {
    //         index_data[i] = rnd.Next();
    //     }
    //     std::sort(index_data, index_data + size);
    //     const int epsilon = 16; // space-time trade-off parameter
    //     PGMIndex<uint64_t, epsilon> index(index_data, index_data + size);
    //     std::cout << "Index segmet size is : " << index.segments_count() << std::endl;
    //     std::vector<double> pgmDurations;
    //     for(int i = 0; i < size; i ++) {
    //         auto startTime = std::chrono::system_clock::now();
    //         auto range = index.search(index_data[i]);
    //         int pos = std::lower_bound(index_data + range.lo,  index_data + range.hi, index_data[i]) - index_data;
    //         auto endTime = std::chrono::system_clock::now();
    //         uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
    //         pgmDurations.push_back(1.0 * ns);
    //         assert(pos == i);
    //     }
    //     auto summaryStats = [](const std::vector<double> &durations, const char *name = "PGM") {
    //         double average = std::accumulate(durations.cbegin(), durations.cend() - 1, 0.0) / durations.size();
    //         auto minmax = std::minmax(durations.cbegin(), durations.cend() - 1);

    //         std::cout << "[" << name << "]: " << "Min: " << *minmax.first << std::endl;
    //         std::cout << "[" << name << "]: " << "Average: " << average << std::endl;
    //         std::cout << "[" << name << "]: " << "Max: " << *minmax.second << std::endl;
    //     };
    //     summaryStats(pgmDurations, "PGM");
    //     free(index_data);
    // }
    // pgm_dynamic_test();
    // rmi_model_test();
    letree_test();
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
    int test_num =100000;
    std::vector<uint64_t> keys;
    for(int i = 0; i < test_num; i ++) {
        uint64_t key = rnd.Next();
        db->insert(key, (char *)key);
        keys.push_back(key);
    }
    db->trans_to_read();
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

void rmi_model_test() {
    int size = 1000000;
    Random rnd(0, UINT64_MAX - 1, 8);
    uint64_t *index_data = (uint64_t *)malloc(size * sizeof(uint64_t));
    for(int i = 0; i < size; i++) {
        index_data[i] = rnd.Next();
    }
    std::sort(index_data, index_data + size); 

    LearnModel::rmi_model<uint64_t> rmi;

    rmi.init(index_data, size, size / 100);
    int max_error = 0;
    std::map<int, int> error_counter;
    for(int i = 0; i < size ; i++) {
        int pos = rmi.predict(index_data[i]);
        int error = std::max(std::abs(pos - i), max_error);
        // if(std::abs(pos - i) > 100) {
        //     std::cout << "[" << i << "] Predict pos: " << pos << std::endl;
        //     rmi.predict(index_data[i]);
        //     // exit(0);
        // }
        error_counter[error] ++;
        // std::cout << "[" << i << "] Predict pos: " << pos << std::endl;
    }
    int total_count;
    for(auto err:error_counter) {
        total_count += err.second;
        std::cerr << "Error[" << err.first << "]: " << err.second << ", " 
            << 100.0 * total_count / size  << "%\n"; 
    }
    // int pos = rmi.predict(index_data[0]);

    // std::cout << "stage 3 : " << pos << std::endl;
    // pos = rmi.stage_2_predict(index_data[10]);

    // std::cout << "stage 2 : " << pos << std::endl;
    std::cout << "Max error: " << max_error << std::endl;
}

void letree_test() {
    using combotree::letree;
    NVM::data_init();
    letree let;
    size_t test_num = 1000000;

    int size = 1000;

    Random rnd(0, UINT64_MAX - 1, 0);
    std::cout << "Group size: " << sizeof(combotree::group) << std::endl;
    // uint64_t *index_data = (uint64_t *)malloc(size * sizeof(uint64_t));
    std::vector<std::pair<uint64_t,uint64_t>> data;
    for(int i = 0; i < size; i++) {
        data.push_back({rnd.Next(), i + 1});
    }
    std::sort(data.begin(), data.end()); 
    let.bulk_load(data);
    {
        int max_error = 0;
        std::cout << "Find entry test\n";
        for(int i = 0; i < size; i++) {
            int pos = let.find_group(data[i].first);
            uint64_t value;
            auto ret = let.Get(data[i].first, value);
            if(!ret) {
                std::cerr << "Get [" << i << "]faild\n";
                auto ret = let.Get(data[i].first, value);
            }
        }
        std::cout << "Max error: " << max_error << std::endl;
    }
    {
        // Put test
        Random rnd(0, UINT64_MAX - 1, 8);
        std::cout << "Put test: \n";
        for(size_t i = 0; i < test_num; i ++) {
            let.Put(rnd.Next(), i * i + 1);
        }
    }
    let.ExpandTree();
    let.Show();
    {
        // Get test
        Random rnd(0, UINT64_MAX - 1, 8);
        std::cout << "Get test: \n";
        for(size_t i = 0; i < test_num; i ++) {
            uint64_t value;
            uint64_t key = rnd.Next();
            auto ret = let.Get(key, value);
            if(!ret || value != (i * i + 1))  {
                std::cerr << "Get [" << i << "]faild\n";
                auto ret = let.Get(key, value);
                assert(0);
            }
        }
        // Common::g_metic.show_metic();
    }
    {
        std::cout << "Iter test:\n";
        letree::Iter it(&let);
        int i = 0;
        while(!it.end() && i < 100) {
            std::cout << "[" << i ++ << "]: " << it.key() << ".\n ";
            it.next();
        }
    }
    {
        // Put test
        Random rnd(0, UINT64_MAX - 1, 18);
        std::cout << "Put test: \n";
        for(size_t i = 0; i < test_num * 10; i ++) {
            let.Put(rnd.Next(), i * i + 1);
        }
    }
    {
        // Get test
        Random rnd(0, UINT64_MAX - 1, 18);
        std::cout << "Get test: \n";
        for(size_t i = 0; i < test_num * 10; i ++) {
            uint64_t value;
            uint64_t key = rnd.Next();
            auto ret = let.Get(key, value);
            if(!ret) {
                std::cerr << "Get [" << i << "]faild\n";
                auto ret = let.Get(key, value);
                assert(0);
            }
        }
        // Common::g_metic.show_metic();
    }
    // int max_error = 0;
    // for(int i = 0; i < size; i++) {
    //     int pos = let.find_entry(data[i].first);
    //     max_error = std::max(std::abs(pos - i), max_error);
    //     // std::cout << "[" << i << "] Predict pos: " << pos << std::endl;
    // }
    // std::cout << "Max error: " << max_error << std::endl;
}