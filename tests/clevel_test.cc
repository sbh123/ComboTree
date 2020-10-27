#include <iostream>
#include <cstdlib>
#include <cassert>
#include <vector>
#include "../src/clevel.h"
#include "random.h"

using combotree::CLevel;

#define TEST_SIZE 10000000

int main(void) {
  void* base_addr = malloc(TEST_SIZE * 40);
  std::cout << "begin addr: " << base_addr << std::endl;
  std::cout << "end addr:   " << (void*)((char*)base_addr + (TEST_SIZE * 40)) << std::endl;
  CLevel::MemControl mem(base_addr);

  CLevel clevel;
  clevel.Setup(&mem, 4);

  std::vector<uint64_t> keys;
  combotree::Random rnd(0, TEST_SIZE * 16);

  for (int i = 0; i < TEST_SIZE; ++i) {
    keys.emplace_back(rnd.Next());
  }

  for (int i = 0; i < TEST_SIZE; ++i) {
    assert(clevel.Put(&mem, keys[i], keys[i]) == true);
  }

  for (int i = 0; i < TEST_SIZE; ++i) {
    uint64_t value;
    assert(clevel.Get(&mem, keys[i], value) == true);
    assert(value == keys[i]);
  }

  // for (int i = TEST_SIZE; i < TEST_SIZE + 1000; ++i) {
  //   uint64_t value;
  //   assert(clevel.Get(&mem, i, value) == false);
  // }

  // for (int i = 1000; i < 10000; ++i) {
  //   uint64_t value;
  //   assert(clevel.Delete(&mem, i, &value) == true);
  //   assert(value == i);
  // }

  // for (int i = 1000; i < 10000; ++i) {
  //   uint64_t value;
  //   assert(clevel.Get(&mem, i, value) == false);
  // }

  return 0;
}