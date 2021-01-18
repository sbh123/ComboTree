#include <iostream>
#include <vector>
#include "../src/combotree_config.h"
// #include "../src/bucket.h"
#include "../src/pointer_bentry.h"
#include "nvm_alloc.h"
#include "random.h"

using combotree::Buncket;
using combotree::PointerBEntry;
using combotree::status;
using combotree::Random;
using combotree::CLevel;

using namespace std;

#define ONE_MIB (1UL << 20)

void Buncket_test();
void PointerBEntry_test();

int main() {
    // Buncket_test();
    PointerBEntry_test();
    return 0;
}

void Buncket_test() {
    Random rnd(0, UINT64_MAX - 1);
    uint64_t main_key = rnd.Next();
    Buncket<256> bucket(main_key, 6);
    std::cout << "Buncket size: " << sizeof(Buncket<256>) << std::endl;
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
        Buncket<256>::Iter it(&bucket, main_key);
        std::cout << "Iter test." << std::endl;
        do {
            std::cout << "Key: " << it.key() << ", value: " << it.value() << std::endl;
        }
        while(it.next());
    }
    {
        Buncket<256> *next;
        uint64_t split_key;
        int prefix_len;
        bucket.Expand_(&mem, next, split_key, prefix_len);
        assert(bucket.Next() == next);
        std::cout << "Expand test." << std::endl;
        {
            Buncket<256>::Iter it(&bucket, main_key);
            std::cout << "Expand left." << std::endl;
            do {
                std::cout << "Key: " << it.key() << ", value: " << it.value() << std::endl;
            }
            while(it.next());   
        }
        {
            Buncket<256>::Iter it(next, main_key);
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
    Buncket<256> bucket(main_key, 6);
    std::cout << "Buncket size: " << sizeof(Buncket<256>) << std::endl;
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
