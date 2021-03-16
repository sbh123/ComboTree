#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <future>
#include <sys/unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/mman.h>
#include <memory.h>

#include "nvm_alloc.h"
#include "random.h"
#include "ycsb/ycsb-c.h"
#include "../src/combotree_config.h"
#include "../src/pmem.h"

using namespace NVM;

const size_t opsizes[] = {
    8, 16, 32, 64, 128, 256, 512, 1024
};

#define ArrayLen(arry) (sizeof(arry) / sizeof(arry[0]))

void nvm_flush(const void *addr, size_t len) {

}
void NVM_DRAM_TEST(size_t size, size_t operations, bool NVM);

int main()
{
    NVM_DRAM_TEST(4UL << 30, 1e6, true);
    return 0;
}
// Dram 测试的时候
// #先查看系统有多少已经预留的大页内存
// # cat /proc/meminfo |grep -i huge
// #预留192个大页
// # sysctl vm.nr_hugepages=4096
// #查看是否预留成功
// # cat /proc/meminfo |grep -i huge

void NVM_DRAM_TEST(size_t size, size_t operations, bool NVM) {
    void *base_addr = nullptr;
    size_t map_len = 0;
    if(NVM) {
        base_addr = PmemMapFile(COMMON_PMEM_FILE, size, &map_len);
    } else {
        // base_addr = mmap(NULL, size, PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS | 0x40000, -1, 0);
        // if (base_addr == MAP_FAILED) {
        //     perror("map mem");
        //     base_addr = NULL;
        //     return ;
        // }
        base_addr = malloc(size);
    }   
    // 随机读
    for(size_t i = 0; i < ArrayLen(opsizes); i ++) {
        pmem_deep_drain(base_addr, map_len);
        size_t opsize = opsizes[i];
        size_t mem_arrys = size / opsize;
        combotree::Random rnd(0, mem_arrys - 1);
        void *buf = malloc(opsize);
        utils::ChronoTimer timer;
        timer.Start();
        for(size_t j = 0; j < operations; j ++) {
            memcpy(buf, (char *)base_addr + (opsize * rnd.Next()), opsize);
        }
        auto duration = timer.End<std::chrono::nanoseconds>();
        std::cout << "average read " << opsize <<  " bytes latency: " << 1.0 * duration / operations << " ns." <<std::endl;
        std::cout << "average read " << opsize <<  " bytes kiops: " << operations / (1.0 * duration / 1e6) << std::endl;
        free(buf);
        sleep(10);
    }

    // 随机写
    for(size_t i = 0; i < ArrayLen(opsizes); i ++) {
        size_t opsize = opsizes[i];
        size_t mem_arrys = size / opsize;
        combotree::Random rnd(0, mem_arrys - 1);
        void *buf = malloc(opsize);
        utils::ChronoTimer timer;
        timer.Start();
        for(size_t j = 0; j < operations; j ++) {
            size_t addr = rnd.Next();
            if(NVM) {
                pmem_memcpy_persist((char *)base_addr + (opsize * addr), buf, opsize);
            } else {
                memcpy((char *)base_addr + (opsize * addr), buf, opsize);
            }
        }
        auto duration = timer.End<std::chrono::nanoseconds>();
        std::cout << "average write " << opsize <<  " bytes latency: " << 1.0 * duration / operations << " ns." <<std::endl;
        std::cout << "average write " << opsize <<  " bytes kiops: " << operations / (1.0 * duration / 1e6) << std::endl;
        free(buf);
        sleep(10);
    }

    if(NVM) {
        pmem_unmap(base_addr, map_len);
    } else {
        // munmap(base_addr, size);
        free(base_addr);
    }
}