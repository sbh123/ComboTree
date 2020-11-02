#include <iostream>
#include <fstream>
#include <vector>
#include "random.h"

using combotree::Random;

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << "usage: " << argv[0] << " <data_size>" << std::endl;
    return 0;
  }

  uint64_t data_size = atoll(argv[1]);

  std::cout << "data size: " << data_size << std::endl;

  Random rnd(0, data_size-1);
  std::vector<uint64_t> key;
  for (uint64_t i = 0; i < data_size; ++i)
    key.push_back(i);
  for (uint64_t i = 0; i < data_size; ++i)
    std::swap(key[i],key[rnd.Next()]);

  std::ofstream data("./data.dat");

  for (auto &k : key) {
    data << k << std::endl;
  }

  data.close();
  return 0;
}