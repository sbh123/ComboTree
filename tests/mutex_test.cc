#include <iostream>
#include <shared_mutex>
#include "timer.h"

#define MUTEX_SIZE  10000000
#define TEST_SIZE   10000000

int a;

int main(void) {
  std::shared_mutex* mutex = new std::shared_mutex[MUTEX_SIZE];
  combotree::Timer timer;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    std::shared_lock<std::shared_mutex> lock(mutex[i % MUTEX_SIZE]);
    a++;
  }
  std::cout << timer.Stop() << std::endl;

  return 0;
}