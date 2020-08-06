#include <libpmemobj++/make_persistent_atomic.hpp>
#include <filesystem>
#include <iostream>
#include "timer.h"

using namespace std;
using pmem::obj::persistent_ptr;
using pmem::obj::make_persistent_atomic;
using combotree::Timer;

#define PATH        "/mnt/pmem0/persistent"
#define TEST_SIZE   10000000
#define PTR(type, offset) ((type*)(base_addr + (offset & 0x3FFFFFFFFFFFFFFF)))

struct A {
  uint64_t a;
  uint64_t b;
  int c;
  std::string d;
};

int main(void) {
  std::filesystem::remove(PATH);
  auto pop = pmem::obj::pool_base::create(PATH, "Ptr Test",
                                          PMEMOBJ_MIN_POOL * 128, 0666);
  persistent_ptr<A> a_ptr, b_ptr;
  uint64_t a_offset;
  uint64_t base_addr;
  make_persistent_atomic<A>(pop, b_ptr);
  base_addr = (uint64_t)b_ptr.get() - b_ptr.raw().off;
  a_offset = b_ptr.raw().off;

  cout << a_offset << endl;

  make_persistent_atomic<A>(pop, a_ptr);

  Timer timer;

  timer.Start();
  A* ptr = a_ptr.get();
  for (int i = 0; i < TEST_SIZE; ++i) {
    ptr->a++;
    ptr->b++;
    ptr->c++;
  }
  cout << timer.Stop() << endl;

  timer.Start();
  assert(b_ptr.raw().pool_uuid_lo == a_ptr.raw().pool_uuid_lo);
  a_offset |= 0xC000000000000000UL;
  for (int i = 0; i < TEST_SIZE; ++i) {
    ptr = PTR(A, a_offset);
    // assert(b_ptr.get() == ptr);
    ptr->a++;
    ptr->b++;
    ptr->c++;
  }
  cout << timer.Stop() << endl;

  return 0;
}