#pragma once

#include <random>
#include <chrono>

namespace combotree {

class RandomUniformUint64 {
 public:
  RandomUniformUint64() {
    // https://stackoverflow.com/a/13446015/7640227
    std::random_device rd;
    // seed value is designed specifically to make initialization
    // parameters of std::mt19937 (instance of std::mersenne_twister_engine<>)
    // different across executions of application
    std::mt19937_64::result_type seed = rd() ^ (
            (std::mt19937_64::result_type)
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()
                ).count() +
            (std::mt19937_64::result_type)
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch()
                ).count() );
    gen_.seed(seed);
  }

  uint64_t Next() {
    return dist_(gen_) & 0x3FFFFFFFFFFFFFFFUL;
  }

 private:
  std::mt19937_64 gen_;
  std::uniform_int_distribution<uint64_t> dist_;
};

} // namespace combotree