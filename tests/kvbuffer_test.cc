#include "../src/kvbuffer.h"

using namespace combotree;

int test(void) {
  KVBuffer<112,6> buf;
  volatile int a = buf.value(0);
  return 0;
}