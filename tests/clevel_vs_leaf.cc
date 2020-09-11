#include <iostream>
#include <filesystem>
#include <mutex>
#include "clevel.h"
#include "slab.h"
#include "random.h"

using combotree::CLevel;
using combotree::CLevel::Mutex;
using combotree::Status;
using combotree::Slab;
using combotree::CLevel::LeafNode;
using namespace std;

#define PATH      "/mnt/pmem0/persistent"
#define TREE_NUM  100000
#define TREE_SIZE 3

int main(void) {
  std::filesystem::remove(PATH);
  auto pop = pmem::obj::pool_base::create(PATH, "CLevel Test",
                                          PMEMOBJ_MIN_POOL * 128, 0666);
  Slab<CLevel::LeafNode>* slab = new combotree::Slab<CLevel::LeafNode>(pop, 1000);
  CLevel::MemoryManagement mem(pop, slab);

  CLevel* db[TREE_NUM];
  LeafNode* nodes[TREE_NUM];
  Mutex mutex[TREE_NUM]

  for (int i = 0; i < TREE_NUM; ++i) {
    db[i] = new CLevel();
    db[i]->InitLeaf(&mem);
    db[i]->mutex_.GetLeafMutex(0);
    nodes[i] = mem.NewLeafNode();
    nodes[i].id = 0;
    mutex[i].GetLeafMutex(0);
  }

  combotree::RandomUniformUint64 rnd;
  uint64_t key[TREE_SIZE];
  for (int i = 0; i < TREE_SIZE; ++i) {
    key[i] = rnd.Next();
  }

  combotree::Timer timer;

  timer.Start();
  for (int i = 0; i < TREE_NUM; ++i) {
    for (int j = 0; j < TREE_SIZE; ++j) {
      db[i]->Insert(&mem, key[j], key[j]);
    }
  }
  std::cout << "CLevel elapsed time: " << timer.End() << std::endl;

  timer.Start();
  for (int i = 0; i < TREE_NUM; ++i) {
    for (int j = 0; j < TREE_SIZE; ++j) {
      nodes[i]->Insert(&mem, mutex[i], key[j], key[j]);
    }
  }
  std::cout << "LeafNode elapsed time: " << timer.End() << std::endl;

  return 0;
}