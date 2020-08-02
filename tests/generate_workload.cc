#include <fstream>
#include <set>
#include <assert.h>
#include "random.h"

using namespace std;

#define WORKLOAD_SIZE 1000000

int main(void) {
  fstream f("/home/qyzhang/Projects/ComboTree/build/workload.txt", ios::trunc | ios::out);
  combotree::RandomUniformUint64 rnd;
  set<uint64_t> key_set;
  for (int i = 0; i < WORKLOAD_SIZE; ++i) {
    uint64_t key = rnd.Next();
    // if (key_set.count(key)) {
    //   i--;
    //   continue;
    // } else {
      key_set.emplace(key);
      f << key << endl;
    // }
  }
  f.close();
  return 0;
}