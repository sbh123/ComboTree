#include <iostream>
#include <filesystem>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/make_persistent_array_atomic.hpp>
#include <libpmem.h>
#include "timer.h"

struct A {
  int a;
  uint64_t b;
  std::string c;
  uint64_t d;
  double e;
};

#define PATH        "/mnt/pmem0/persistent"
#define TEST_SIZE   1000000

using namespace pmem::obj;

int main(void) {
  std::filesystem::remove(PATH);
  auto pop = pmem::obj::pool_base::create(PATH, "CLevel Test",
                                          PMEMOBJ_MIN_POOL * 128, 0666);
  combotree::Timer timer;
  persistent_ptr<A> a;
  make_persistent_atomic<A>(pop, a);

  persistent_ptr<A[]> a_array;
  make_persistent_atomic<A[]>(pop, a_array, TEST_SIZE);

  A* b_array = new A[TEST_SIZE];

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    a_array[i].a++;
  }
  std::cout << timer.Stop() << std::endl;

  A* b = new A();
  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    b_array[i].a++;
  }
  std::cout << timer.Stop() << std::endl;
}