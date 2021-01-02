#include <iostream>
#include <fstream>
#include <cassert>
#include <iomanip>
#include <sstream>
#include <vector>
#include <algorithm>

#define USE_STD_ITER

#include "pgm_index.h"
#include "random.h"


using combotree::Random;
using combotree::Entry;
int main() {
    Random rnd(0, UINT64_MAX - 1);
    {
        int size = 10;
        uint64_t *index_data = (uint64_t *)malloc(size * sizeof(uint64_t));
        // for(int i = 0; i < size; i++) {
        //     index_data[i] = rnd.Next() % 50;
        // }
        // std::sort(index_data, index_data + size);
        index_data[0] = 6; index_data[1] = 9, index_data[2] = 11;
        index_data[3] = 13; index_data[4] = 14, index_data[5] = 15;
        index_data[6] = 18; index_data[7] = 27, index_data[8] = 37;
        index_data[9] = 45; 
        // uint64_t key = index_data[size / 2];
        const int epsilon = 2; // space-time trade-off parameter
        pgm::PGMIndex<uint64_t, epsilon> index(index_data, index_data + size);
        // auto range = index.search(key);
        // std::cout << "Key is : " << key << std::endl;
        // std::cout << "Index segmet size is : " << index.segments_count() << std::endl;
        // std::cout << "Range: " << range.lo << ":" << range.hi << std::endl;
        // for(int i = range.lo; i <  range.hi; i++) {
        //     std::cout << "Entry: " << i << ":" << index_data[i] << std::endl;
        // }
        // int pos = std::lower_bound(index_data + range.lo,  index_data + range.hi, key) - index_data;
        // std::cout << "Find key pos is : " << pos << std::endl;
        free(index_data);
    }
    return 0;
}