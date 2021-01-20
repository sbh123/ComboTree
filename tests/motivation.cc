#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <future>
#include "ycsb/ycsb-c.h"
#include "nvm_alloc.h"
#include "common_time.h"
#include "../src/combotree_config.h"
#include "combotree/combotree.h"
#include "fast-fair/btree.h"
#include "learnindex/pgm_index_dynamic.hpp"
#include "learnindex/rmi.h"
#include "alex/alex.h"
#include "stx/btree_map.h"

void FastFairTest(size_t load, size_t operations);
int main()
{
    return 0;
}

void FastFairTest(size_t load, size_t operations) {
    
}