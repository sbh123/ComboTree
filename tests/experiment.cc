#include <iostream>
#include <chrono>

using namespace std;

struct Test {
  uint64_t i;
  char a[];
};

class Timer {
 public:
  void Start() { start_ = std::chrono::high_resolution_clock::now(); }

  double End() {
    end_ = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end_ - start_;
    return duration.count() * 1000000;
  }

 private:
  std::chrono::time_point<std::chrono::high_resolution_clock> start_;
  std::chrono::time_point<std::chrono::high_resolution_clock> end_;
};

int main(void) {
  cout << sizeof(Test) << endl;

  char buf[1024];

  const int TEST_SIZE = 10000000;

  cout << hex << (uint64_t)buf << endl;

  Timer timer;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    *(uint64_t*)&buf[2] = 1002;
  }
  cout << timer.End() << endl;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    *(uint64_t*)&buf[2] = 1002;
  }
  cout << timer.End() << endl;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    *(uint64_t*)&buf[1] = 1002;
  }
  cout << timer.End() << endl;

  timer.Start();
  for (int i = 0; i < TEST_SIZE; ++i) {
    *(uint64_t*)&buf[0] = 1002;
  }
  cout << timer.End() << endl;

  return 0;
}