#include <iostream>
#include <shared_mutex>
#include <mutex>
#include <atomic>
#include <vector>
#include <thread>
#include "random.h"
#include "timer.h"

#define MUTEX_SIZE    1
#define TEST_SIZE     10000000
#define THREAD_NUM    32

std::shared_mutex shared_mut[MUTEX_SIZE];
std::mutex mut[MUTEX_SIZE];
int locked_data[MUTEX_SIZE];
std::atomic<int> lockfree_data[MUTEX_SIZE];

void locked_main(void) {
  combotree::RandomUniformUint64 rnd;
  for (int i = 0; i < TEST_SIZE; ++i) {
    int index = rnd.Next() % MUTEX_SIZE;
    std::lock_guard<std::mutex> lock(mut[index]);
    locked_data[index]++;
  }
}

void lockfree_main(void) {
  combotree::RandomUniformUint64 rnd;
  for (int i = 0; i < TEST_SIZE; ++i) {
    int index = rnd.Next() % MUTEX_SIZE;
    lockfree_data[index]++;
  }
}

int main(void) {
  std::cout << lockfree_data[0].is_always_lock_free << std::endl;

  combotree::Timer timer;
  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    std::lock_guard<std::shared_mutex> lock(shared_mut[i % MUTEX_SIZE]);
    locked_data[i % MUTEX_SIZE]++;
  }
  std::cout << timer.Stop() << std::endl;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    std::lock_guard<std::mutex> lock(mut[i % MUTEX_SIZE]);
    locked_data[i % MUTEX_SIZE]++;
  }
  std::cout << timer.Stop() << std::endl;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    lockfree_data[i % MUTEX_SIZE]++;
  }
  std::cout << timer.Stop() << std::endl;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    locked_data[i % MUTEX_SIZE]++;
  }
  std::cout << timer.Stop() << std::endl;

  std::vector<std::thread> threads;
  timer.Start();
  for (int i = 0; i < THREAD_NUM; ++i) {
    threads.emplace_back(locked_main);
  }
  for (auto& t : threads) {
    if (t.joinable())
      t.join();
  }
  std::cout << timer.Stop() << std::endl;

  threads.clear();
  timer.Start();
  for (int i = 0; i < THREAD_NUM; ++i) {
    threads.emplace_back(lockfree_main);
  }
  for (auto& t : threads) {
    if (t.joinable())
      t.join();
  }
  std::cout << timer.Stop() << std::endl;

  return 0;
}