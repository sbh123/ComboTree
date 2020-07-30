#include "clevel.h"

int main(void) {
  combotree::CLevel clevel;

  for (int i = 0; i < 10; ++i) {
    clevel.Insert(i, i);
  }

  clevel.Delete(5);
  clevel.Delete(3);
  clevel.Delete(2);
  clevel.Delete(1);

  // for (int i = 0; i < 10; ++i) {
  //   uint64_t value;
  //   bool find = clevel.Get(i, value);
  //   if (i != 5)
  //     assert(find && value == i);
  //   else
  //     assert(!find);
  // }

  for (int i = 10; i < 20; ++i) {
    clevel.Insert(i, i);
  }

  for (int i = 10; i < 20; ++i) {
    uint64_t value;
    bool find = clevel.Get(i, value);
    assert(find && value == (uint64_t)i);
  }

  clevel.Delete(14);

  clevel.Insert(14, 14);

    uint64_t value;
    bool find = clevel.Get(14, value);
    assert(find && value == 14UL);

  return 0;
}