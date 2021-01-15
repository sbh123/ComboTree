#include <iostream>
#include <cstdlib>
#include <cassert>
#include <vector>

#include "random.h"
#include "../src/clevel.h"
#include "../src/bentry.h"

using combotree::CLevel;
using combotree::Random;
using combotree::NobufBEntry;

const int TEST_SIZE = 300000;

void tmpbentry_test();

int main(void) {
  std::vector<uint64_t> key;
  Random rnd(0, TEST_SIZE-1);

  for (int i = 0; i < TEST_SIZE; ++i)
    key.push_back(i);

  for (int i = 0; i < TEST_SIZE; ++i)
    std::swap(key[i], key[rnd.Next()]);

  void* base_addr = malloc(TEST_SIZE * 40);
  std::cout << "begin addr: " << base_addr << std::endl;
  std::cout << "end addr:   " << (void*)((char*)base_addr + (TEST_SIZE * 40)) << std::endl;
  CLevel::MemControl mem(base_addr, TEST_SIZE * 40);

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
  tmpbentry_test();
  return 0;
}

void tmpbentry_test()
{
  std::vector<uint64_t> key;
  Random rnd(0, TEST_SIZE-1);

  std::cout << "NobufBentry size: " << sizeof(combotree::NobufBEntry) << std::endl;
  for (int i = 0; i < TEST_SIZE; ++i)
    key.push_back(i);

  for (int i = 0; i < TEST_SIZE; ++i)
    std::swap(key[i], key[rnd.Next()]);
  
  combotree::NobufBEntry *bentry = nullptr;
  void* base_addr = malloc(TEST_SIZE * 40);
  std::cout << "begin addr: " << base_addr << std::endl;
  std::cout << "end addr:   " << (void*)((char*)base_addr + (TEST_SIZE * 40)) << std::endl;
  CLevel::MemControl mem(base_addr, TEST_SIZE * 40);

  bentry = new NobufBEntry(0, 4, &mem);
  auto summaryStats = [](const std::vector<double> &durations, const char *name = "PGM") {
      double average = std::accumulate(durations.cbegin(), durations.cend() - 1, 0.0) / durations.size();
      auto minmax = std::minmax(durations.cbegin(), durations.cend() - 1);

      std::cout << "[" << name << "]: " << "Min: " << *minmax.first << std::endl;
      std::cout << "[" << name << "]: " << "Average: " << average << std::endl;
      std::cout << "[" << name << "]: " << "Max: " << *minmax.second << std::endl;
  };
  std::vector<double> durations;
  for (int i = 0; i < TEST_SIZE; ++i) {
    auto startTime = std::chrono::system_clock::now();
    assert(bentry->Put(&mem, key[i], key[i]) == true);
    auto endTime = std::chrono::system_clock::now();
    uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
    durations.push_back(1.0 * ns);
  }
  summaryStats(durations, "Clevel Insert");
  durations.clear();

  for (int i = 0; i < TEST_SIZE; ++i) {
    uint64_t value;
    auto startTime = std::chrono::system_clock::now();
    assert(bentry->Get(&mem, key[i], value) == true);
    assert(value == key[i]);
    auto endTime = std::chrono::system_clock::now();
    uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
    durations.push_back(1.0 * ns);
  }
  summaryStats(durations, "Clevel Get");

  durations.clear();

  NobufBEntry::Iter iter(bentry, &mem, 1000);
  for (uint64_t i = 1000; i < TEST_SIZE; ++i) {
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
    assert(bentry->Get(&mem, i, value) == false);
  }

  for (uint64_t i = 0; i < TEST_SIZE / 2; ++i) {
    uint64_t value;
    assert(bentry->Delete(&mem, i, &value) == true);
    assert(value == i);
  }

  for (uint64_t i = 0; i < TEST_SIZE / 2; ++i) {
    uint64_t value;
    assert(bentry->Get(&mem, i, value) == false);
  }

  for (uint64_t i = TEST_SIZE / 2; i < TEST_SIZE; ++i) {
    uint64_t value;
    assert(bentry->Get(&mem, i, value) == true);
    assert(value == i);
  }

  delete bentry;
}