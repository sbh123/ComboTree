#include <iostream>
#include <cstdlib>
#include <cassert>
#include <vector>
#include "clevel.h"
#include "random.h"

using combotree::CLevel;
using combotree::Random;

#define TEST_SIZE 200000

int main(void) {
  std::vector<uint64_t> key;
  Random rnd(0, TEST_SIZE-1);

  for (int i = 0; i < TEST_SIZE; ++i)
    key.push_back(i);

  for (int i = 0; i < TEST_SIZE; ++i)
    std::swap(key[i], key[rnd.Next()]);

  void* base_addr = malloc(TEST_SIZE * 400);
  std::cout << "begin addr: " << base_addr << std::endl;
  std::cout << "end addr:   " << (void*)((char*)base_addr + (TEST_SIZE * 40)) << std::endl;
  CLevel::MemControl mem(base_addr, TEST_SIZE * 400);

  CLevel clevel;
  clevel.Setup(&mem, 4);

  for (int i = 0; i < TEST_SIZE; ++i) {
    assert(clevel.Put(&mem, key[i], key[i]) == true);
  }

  for (int i = 0; i < TEST_SIZE; ++i) {
    uint64_t value;
    assert(clevel.Get(&mem, key[i], value) == true);
    assert(value == key[i]);
  }

  CLevel::Iter aiter(&clevel, &mem, 0);
  uint64_t last = aiter.key();
  while (aiter.next()) {
    assert(aiter.key() > last);
    last = aiter.key();
  }

  CLevel::Iter iter(&clevel, &mem, 0);
  for (uint64_t i = 0; i < TEST_SIZE; ++i) {
    assert(iter.key() == i);
    assert(iter.value() == i);
    if (i != TEST_SIZE - 1)
      assert(iter.next() == true);
    else
      assert(iter.next() == false);
  }
  assert(iter.end());

  for (uint64_t i = TEST_SIZE; i < TEST_SIZE + 1000; ++i) {
    uint64_t value;
    assert(clevel.Get(&mem, i, value) == false);
  }

  for (uint64_t i = 0; i < TEST_SIZE / 2; ++i) {
    uint64_t value;
    assert(clevel.Delete(&mem, i, &value) == true);
    assert(value == i);
  }

  for (uint64_t i = 0; i < TEST_SIZE / 2; ++i) {
    uint64_t value;
    assert(clevel.Get(&mem, i, value) == false);
  }

  for (uint64_t i = TEST_SIZE / 2; i < TEST_SIZE; ++i) {
    uint64_t value;
    assert(clevel.Get(&mem, i, value) == true);
    assert(value == i);
  }

  return 0;
}