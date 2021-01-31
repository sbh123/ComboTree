#include <iostream>
#include <vector>
#include <map>
#include <x86intrin.h>
#include "../src/combotree_config.h"
// #include "../src/bucket.h"
#include "../src/pointer_bentry.h"
#include "../src/learn_group.h"
#include "nvm_alloc.h"
#include "bitops.h"
#include "random.h"

using combotree::Buncket;
using combotree::PointerBEntry;
using combotree::buncket_t;
using combotree::status;
using combotree::Random;
using combotree::CLevel;
using combotree::Debug;

using namespace std;

#define ONE_MIB (1UL << 20)

void Buncket_test();
void PointerBEntry_test();
void bit_test();
void Learn_Group_test();

int main() {
    NVM::env_init();
    // Buncket_test();
    // PointerBEntry_test();
    // bit_test();
    Learn_Group_test();
    NVM::env_exit();
    return 0;
}

void Buncket_test() {
    Random rnd(0, UINT64_MAX - 1);
    uint64_t main_key = rnd.Next();
    buncket_t bucket(main_key, 6);
    std::cout << "Buncket size: " << sizeof(buncket_t) << std::endl;
    CLevel::MemControl mem(COMMON_PMEM_FILE, ONE_MIB);
    vector<uint64_t> keys;
    status ret;
    for(int i = 0; i < 30; i ++) {
        uint64_t key = (rnd.Next() & 0xffffUL) | (main_key & ~(0xffffUL));
        ret = bucket.Put(nullptr, key, key);
        // if(ret != status::OK) {
            std::cout << "Put Key(" << key << "), ret: " << ret << std::endl;   
        // } 
        keys.push_back(key);
    }
    for(int i = 0; i < 20; i ++) {
        std::cout << "Key: " << bucket.key(i) << ", value: " << bucket.value(i) << std::endl;
    }
    for(int i = 0; i < 5; i ++) {
        bucket.Delete(nullptr, keys[i], nullptr);
    }
    for(int i = 0; i < 10; i ++) {
        uint64_t value;
        ret = bucket.Get(nullptr, keys[i], value);
        if(ret != status::OK) {
            std::cout << "Get Key failed. (" << keys[i] << ")" << std::endl;   
        } 

    }
    for(int i = 0; i < 5; i ++) {
        uint64_t key = (rnd.Next() & 0xffffUL) | (main_key & ~(0xffffUL));
        bucket.Put(nullptr, key, key);
        keys.push_back(key);
    }
    for(int i = 0; i < 10; i ++) {
        std::cout << "Key: " << bucket.key(i) << ", value: " << bucket.value(i) << std::endl;
    } 
    {
        buncket_t::Iter it(&bucket, main_key);
        std::cout << "Iter test." << std::endl;
        do {
            std::cout << "Key: " << it.key() << ", value: " << it.value() << std::endl;
        }
        while(it.next());
    }
    {
        buncket_t *next;
        uint64_t split_key;
        int prefix_len;
        bucket.Expand_(&mem, next, split_key, prefix_len);
        assert(bucket.Next() == next);
        std::cout << "Expand test." << std::endl;
        {
            buncket_t::Iter it(&bucket, main_key);
            std::cout << "Expand left." << std::endl;
            do {
                std::cout << "Key: " << it.key() << ", value: " << it.value() << std::endl;
            }
            while(it.next());   
        }
        {
            buncket_t::Iter it(next, main_key);
            std::cout << "Expand right." << std::endl;
            do {
                std::cout << "Key: " << it.key() << ", value: " << it.value() << std::endl;
            }
            while(it.next());   
        }
    }
}
void PointerBEntry_test() {
    Random rnd(0, UINT64_MAX - 1);
    uint64_t main_key = rnd.Next();
    buncket_t bucket(main_key, 6);
    std::cout << "Buncket size: " << sizeof(buncket_t) << std::endl;
    CLevel::MemControl mem(COMMON_PMEM_FILE, ONE_MIB);
// PointerBEntry test
    std::cout << "PointerBEntry size " << sizeof(PointerBEntry) << std::endl;
    PointerBEntry *pointer_bentry = new PointerBEntry((main_key & ~(0xffffUL)), 6, &mem);
    vector<uint64_t> keys;
    for(int i = 0; i < 100; i ++) {
        uint64_t key = (rnd.Next() & 0xffffUL) | (main_key & ~(0xffffUL));
        auto ret = pointer_bentry->Put(&mem, key, key);
        if(!ret) {
            std::cout << "Put Keys[" << i << "]: " << key << ", ret: " << ret << std::endl;   
        } 
        keys.push_back(key);
    }

    pointer_bentry->Show(&mem);
    std::cout << "Start get." << std::endl;
    for(int i = 0; i < 100; i ++) {
        uint64_t value;
        auto ret = pointer_bentry->Get(&mem, keys[i], value);
        if(!ret) {
            std::cout << "Pointer BEntry: Get Key failed. (" << keys[i] << ")" << std::endl;    
        }

    }
    {
        // Iter test
        int idx = 0;
        PointerBEntry::Iter it(pointer_bentry, &mem);
        do {
                std::cout << "Iter[" << idx ++ << "]: Key: " << it.key() << ", value: " << it.value() << std::endl;
        }
        while(it.next()); 
    }
    delete pointer_bentry;
}

void bit_test()
{
   uint64_t bitmap = 0; 
   for(int i = 0; i < 20; i ++) {
       int pos = _tzcnt_u64(~bitmap);;
       set_bit(pos, &bitmap);
       std::cout << "pos = " << pos << std::endl;
   }
}

void Learn_Group_test()
{
    combotree::RootModel *root;
    std::vector<uint64_t> key;
    std::vector<std::pair<uint64_t,uint64_t>> exist_kv;
    std::map<uint64_t, uint64_t> right_kv;
    const uint64_t TEST_SIZE = 3000;

    CLevel::MemControl mem(COMMON_PMEM_FILE, 100 * ONE_MIB);
    NVM::data_init();
    Random rnd(0, UINT64_MAX - 1);
    // right_kv.emplace(0, UINT64_MAX);
    for (uint64_t i = 0; i < TEST_SIZE; ++i) {
        uint64_t key = rnd.Next();
        if (right_kv.count(key)) {
            i--;
            continue;
        }
        uint64_t value = rnd.Next();
        right_kv.emplace(key, value);
    }
    exist_kv.assign(right_kv.begin(), right_kv.end());

    root = new combotree::RootModel(45, &mem);
    root->Load(exist_kv);

    for(int i = 0; i < exist_kv.size(); i ++) {
        uint64_t value;
        root->Get(exist_kv[i].first, value);
        if(value != exist_kv[i].second) {
            printf("Get %d key, expect %lu, find %lu.\n", i, exist_kv[i].second, value);
            assert(value == exist_kv[i].second);
        }
    }
    {
        combotree::RootModel::Iter it(root);
        int idx  =0;
        while(!it.end()) {
            std::cout << "pair[" << idx ++ << "] Key: " << it.key() << ", value: " << it.value() << std::endl;
            it.next();
        }
    }
    for(int i = 0; i < 1000000; i ++) {
        uint64_t key = rnd.Next();
        if (right_kv.count(key)) {
            i--;
            continue;
        }
        uint64_t value = rnd.Next();
        exist_kv.push_back({key, value});
        root->Put(key, value);
        right_kv.emplace(key, value);
        // uint64_t new_value;
        // root->Get(key, new_value);
        // if(value != new_value) {
        //     root->Put(key, value);
        //     root->Get(key, new_value);
        //     std::cout << "Failed num: " << i << std::endl;
        //     assert(value == new_value);
        // }
    }

    for(int i = 0; i < exist_kv.size(); i ++) {
        uint64_t value;
        root->Get(exist_kv[i].first, value);
        if(value != exist_kv[i].second) {
            printf("Get %d key, expect %lu, find %lu.\n", i, exist_kv[i].second, value);
            root->Get(exist_kv[i].first, value);
            assert(value == exist_kv[i].second);
        }
    }
    {
        for(int i = 1; i < exist_kv.size() / 100; i ++) {
            uint64_t start_key = rnd.Next();
            combotree::RootModel::Iter iter(root, start_key);
            auto right_iter = right_kv.lower_bound(start_key);
            if(right_iter == right_kv.end()) continue;

            if (right_iter->first != iter.key()) {
                std::cout << i << "key: " << start_key << ", find " << iter.key() << ", expect " << right_iter->first << std::endl;
                combotree::RootModel::Iter iter(root, start_key);
                assert(right_iter->first == iter.key());
            }
            for (int j = 0; j < 100 && right_iter != right_kv.cend(); ++j) {
                assert(!iter.end());
                if(right_iter->first != iter.key()) {
                    combotree::RootModel::Iter it(root, start_key);
                    auto map_it = right_kv.lower_bound(start_key);
                    for (int z = 0; z < 100 ; ++z) {
                        std::cout << "Iter[" << z << "]: Key: " << it.key() << ", value: " << it.value() << std::endl;
                        std::cout << "Map Iter[" << z << "]: Key: " << map_it->first << ", value: " << map_it->second << std::endl;
                        it.next();
                        map_it ++;   
                    }
                    std::cout << "Unexpected." << std::endl;
                }
                assert(right_iter->first == iter.key());
                assert(right_iter->second == iter.value());
                right_iter++;
                iter.next();
            }
        }
        // combotree::RootModel::Iter it(root);
        // int idx  =0;
        // while(!it.end()) {
        //     std::cout << "pair[" << idx ++ << "] Key: " << it.key() << ", value: " << it.value() << std::endl;
        //     it.next();
        // }
    }
    root->Info();
    mem.Usage();
    delete root;
}