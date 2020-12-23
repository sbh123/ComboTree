#include <iostream>
#include <fstream>
#include <cassert>
#include <iomanip>
#include <sstream>
#include <vector>
#include <algorithm>

#include "pgm_index.h"
#include "random.h"


using combotree::Random;
using combotree::Entry;
int main() {
    Random rnd(0, UINT64_MAX - 1);
    {
        int size = 100000;
        uint64_t *index_data = (uint64_t *)malloc(size * sizeof(uint64_t));
        for(int i = 0; i < size; i++) {
            index_data[i] = rnd.Next();
        }
        std::sort(index_data, index_data + size);
        uint64_t key = index_data[size / 2];
        const int epsilon = 16; // space-time trade-off parameter
        pgm::PGMIndex<uint64_t, epsilon> index(index_data, index_data + size);
        auto range = index.search(key);
        std::cout << "Key is : " << key << std::endl;
        std::cout << "Index segmet size is : " << index.segments_count() << std::endl;
        std::cout << "Range: " << range.lo << ":" << range.hi << std::endl;
        for(int i = range.lo; i <  range.hi; i++) {
            std::cout << "Entry: " << i << ":" << index_data[i] << std::endl;
        }
        int pos = std::lower_bound(index_data + range.lo,  index_data + range.hi, key) - index_data;
        std::cout << "Find key pos is : " << pos << std::endl;
        free(index_data);
    }
    return 0;
}