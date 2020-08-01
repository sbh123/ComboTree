#include <iostream>
#include <filesystem>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include "clevel.h"

using pmem::obj::persistent_ptr;
using pmem::obj::make_persistent_atomic;
using combotree::CLevel;

#define POOL_PATH   "/mnt/pmem0/persistent"
#define POOL_LAYOUT "Alloc Test"
#define POOL_SIZE   (PMEMOBJ_MIN_POOL * 400)

class A {
  int a;
  struct {
    int b;
    bool c;
  };

 private:
  struct Iter;
};

struct B {
  B() {}
  B(int a) : a(a) {}
  int a;
  std::string c;
};

struct A::Iter {
  Iter(int a) : a(a), b(a) {}
  int a;
  B b;
};

int main(void) {
  std::filesystem::remove(POOL_PATH);
  auto pop = pmem::obj::pool_base::create(POOL_PATH, POOL_LAYOUT, POOL_SIZE, 0666);

  // persistent_ptr<A> a = nullptr;
  // make_persistent_atomic<A>(pop, a);
  // assert(a != nullptr);
  // std::cout << std::boolalpha;

  // std::cout << "A" << std::endl;
  // std::cout << std::is_trivially_copyable<A>::value << std::endl;
  // std::cout << std::is_standard_layout<A>::value << std::endl;
  // std::cout << std::endl;

  // std::cout << "CLevel" << std::endl;
  // std::cout << std::is_trivially_copyable<CLevel>::value << std::endl;
  // std::cout << std::is_standard_layout<CLevel>::value << std::endl;
  // std::cout << std::endl;

  // std::cout << "LeafNode" << std::endl;
  // std::cout << std::is_trivially_copyable<CLevel::LeafNode>::value << std::endl;
  // std::cout << std::is_standard_layout<CLevel::LeafNode>::value << std::endl;
  // std::cout << std::endl;

  // std::cout << "IndexNode" << std::endl;
  // std::cout << std::is_trivially_copyable<CLevel::IndexNode>::value << std::endl;
  // std::cout << std::is_standard_layout<CLevel::IndexNode>::value << std::endl;
  // std::cout << std::endl;

  // std::cout << "Iter" << std::endl;
  // std::cout << std::is_trivially_copyable<CLevelIter>::value << std::endl;
  // std::cout << std::is_standard_layout<CLevelIter>::value << std::endl;
  // std::cout << std::endl;

  // std::cout << "Entry" << std::endl;
  // std::cout << std::is_trivially_copyable<CLevel::Entry>::value << std::endl;
  // std::cout << std::is_standard_layout<CLevel::Entry>::value << std::endl;
  // std::cout << std::endl;

  // std::cout << "Node" << std::endl;
  // std::cout << std::is_trivially_copyable<Node>::value << std::endl;
  // std::cout << std::is_standard_layout<Node>::value << std::endl;
  // std::cout << std::endl;

  CLevel::SetPoolBase(pop);
  persistent_ptr<CLevel> clevel = nullptr;
  make_persistent_atomic<CLevel>(pop, clevel);
  assert(clevel != nullptr);
}