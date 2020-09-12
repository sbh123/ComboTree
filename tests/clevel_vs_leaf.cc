#include <iostream>
#include <filesystem>
#include <mutex>
#include "clevel.h"
#include "slab.h"
#include "random.h"

using combotree::CLevel;
using combotree::Status;
using combotree::Slab;
using namespace std;

#define PATH      "/mnt/pmem0/persistent"
#define TREE_NUM  100000
#define TREE_SIZE 3

struct Node {
  uint64_t key[8];
  uint64_t value[8];
  int size;

  Node() : size(0) {}

  void Insert(uint64_t new_key, uint64_t new_value) {
    key[size] = new_key;
    value[size] = new_value;
    size++;
  }
};

int main(void) {
  std::filesystem::remove(PATH);
  auto pop = pmem::obj::pool_base::create(PATH, "CLevel Test",
                                          PMEMOBJ_MIN_POOL * 128, 0666);
  Slab<CLevel::LeafNode>* slab = new combotree::Slab<CLevel::LeafNode>(pop, 1000);
  CLevel::MemoryManagement mem(pop, slab);

  CLevel** db = new CLevel*[TREE_NUM];
  CLevel::LeafNode** nodes = new CLevel::LeafNode*[TREE_NUM];
  CLevel::Mutex* mutex = new CLevel::Mutex[TREE_NUM];
  Node* arr;
  std::mutex* arr_mutex = new std::mutex[TREE_NUM];

  for (int i = 0; i < TREE_NUM; ++i) {
    db[i] = new CLevel();
    db[i]->InitLeaf(&mem);
    db[i]->mutex_.GetLeafMutex(0);

    nodes[i] = mem.NewLeafNode();
    nodes[i]->id = 0;
    mutex[i].GetLeafMutex(0);
  }
  pmem::obj::persistent_ptr<Node[]> data;
  pmem::obj::make_persistent_atomic<Node[]>(pop, data, TREE_NUM);
  arr = data.get();

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
      nodes[i]->Insert(&mem, mutex[i], key[j], key[j], nullptr);
    }
  }
  std::cout << "LeafNode elapsed time: " << timer.End() << std::endl;

  timer.Start();
  for (int i = 0; i < TREE_NUM; ++i) {
    for (int j = 0; j < TREE_SIZE; ++j) {
      std::lock_guard<std::mutex> lock(arr_mutex[i]);
      arr[i].Insert(key[j], key[j]);
    }
  }
  std::cout << "Array elapsed time: " << timer.End() << std::endl;

  return 0;
}