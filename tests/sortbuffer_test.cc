#include <assert.h>
#include <iostream>
#include "../src/sortbuffer.h"

using namespace combotree;

uint64_t circular_lshifts(uint64_t src, int start_pos, int count) {
  uint64_t dst = _bzhi_u64(src, start_pos);
  uint64_t need_rotate = src ^ dst;
  need_rotate = (need_rotate << count) | ((need_rotate >> (64-count)) << start_pos);
  return dst | need_rotate;
}

uint64_t circular_rshifts(uint64_t src, int start_pos, int count) {
  uint64_t dst = _bzhi_u64(src, start_pos);
  uint64_t need_rotate = src ^ dst;
  need_rotate = (need_rotate << (64-start_pos-count)) | ((need_rotate << count) >> (2*count));
  return dst | need_rotate;
}


int main(void) {
  SortBuffer<112, 8> buf;
  assert(sizeof(buf) == 112 + 8);

  uint64_t test = 0x0123456789ABCDEFUL;

  uint64_t l = circular_lshifts(test, 3*4, 1*4);
  uint64_t r = circular_rshifts(test, 3*4, 1*4);
  std::cout << l << std::endl << r << std::endl;

  buf.suffix_bytes = 1;
  buf.prefix_bytes = 7;
  buf.max_entries  = buf.MaxEntries();
  buf.entries = 0;

  for (int i = 0; i < 10; ++i)
    buf.Put(i, i, i);

  assert(buf.entries == 10);
  for (int i = 0; i < 10; ++i) {
    uint64_t key = buf.sort_key(i, 0);
    assert(key == i);
    uint64_t value = buf.sort_value(i);
    assert(value == i);
  }

  return 0;
}