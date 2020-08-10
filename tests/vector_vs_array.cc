#include <iostream>
#include <vector>
#include "timer.h"

using combotree::Timer;
using namespace std;

#define ARRAY_SIZE  100
#define TEST_SIZE   1000000

struct A {
  A(uint64_t key, uint64_t value) : key(key), value(value) {}
  A() = default;
  uint64_t key;
  uint64_t value;
};

int main(void) {
  vector<pair<uint64_t, uint64_t>> vec;
  vector<A> key_val_vec;
  uint64_t key[ARRAY_SIZE];
  uint64_t value[ARRAY_SIZE];
  A key_val[ARRAY_SIZE];
  size_t size;

  Timer timer;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    vec.clear();
    vec.reserve(ARRAY_SIZE);
    for (int j = 0; j < ARRAY_SIZE; ++j)
      vec.emplace_back(i, i + 1000);
  }
  cout << timer.Stop() << endl;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    key_val_vec.clear();
    key_val_vec.reserve(ARRAY_SIZE);
    for (int j = 0; j < ARRAY_SIZE; ++j)
      key_val_vec.emplace_back(i, i + 1000);
  }
  cout << timer.Stop() << endl;

  timer.Start();
  vec.reserve(ARRAY_SIZE);
  for (int i = 0; i < TEST_SIZE; ++i) {
    size = 0;
    for (int j = 0; j < ARRAY_SIZE; ++j) {
      key[size] = i;
      value[size++] = i + 1000;
    }
  }
  cout << timer.Stop() << endl;

  timer.Start();
  vec.reserve(ARRAY_SIZE);
  for (int i = 0; i < TEST_SIZE; ++i) {
    size = 0;
    for (int j = 0; j < ARRAY_SIZE; ++j) {
      key_val[size].key = i;
      key_val[size++].value = i + 1000;
    }
  }
  cout << timer.Stop() << endl;
}