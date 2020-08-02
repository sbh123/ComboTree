#include <iostream>
#include <filesystem>
#include <vector>
#include "pmemkv.h"
#include "clevel.h"
#include "timer.h"
#include "random.h"

using namespace combotree;

#define PATH        "/mnt/pmem0/persistent"
#define PMEMKV_PATH "/mnt/pmem0/pmemkv"
#define TEST_SIZE   1000000

int main(void) {
  std::filesystem::remove(PATH);
  auto clevel_pop = pmem::obj::pool_base::create(PATH, "CLevel VS PmemKV",
                                          PMEMOBJ_MIN_POOL * 128, 0666);
  CLevel::SetPoolBase(clevel_pop);

  CLevel* clevel;
  clevel = new CLevel();
  clevel->InitLeaf();

  std::filesystem::remove(PMEMKV_PATH);
  PmemKV* pmemkv;
  pmemkv = new PmemKV(PMEMKV_PATH);

  RandomUniformUint64 rnd;

  std::vector<uint64_t> keys;
  for (int i = 0; i < TEST_SIZE; ++i) {
    keys.emplace_back(rnd.Next());
  }

  uint64_t key, value;
  double duration;
  Timer timer;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    key = keys[i];
    pmemkv->Insert(key, key);
  }
  duration = timer.Stop();
  std::cout << "pmemkv put time: " << duration << std::endl;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    key = keys[i];
    clevel->Insert(key, key);
  }
  duration = timer.Stop();
  std::cout << "clevel put time: " << duration << std::endl;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    key = keys[i];
    pmemkv->Get(key, value);
  }
  duration = timer.Stop();
  std::cout << "pmemkv get time: " << duration << std::endl;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    key = keys[i];
    clevel->Get(key, value);
  }
  duration = timer.Stop();
  std::cout << "clevel get time: " << duration << std::endl;

  return 0;
}