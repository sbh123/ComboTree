#include <iostream>
#include <vector>
#include "../src/combotree_config.h"
#include "../src/bucket.h"
#include "random.h"

using combotree::Bucket;
using combotree::status;
using combotree::Random;
using namespace std;

int main() {
    Random rnd(0, UINT64_MAX - 1);
    uint64_t main_key = rnd.Next();
    Bucket bucket(main_key, 6);
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
    return 0;
}
