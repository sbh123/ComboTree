#include <chrono>
#include <map>

namespace combotree {

class Timer {
 public:
  void Record(std::string name) {
    time_map_.emplace(name, std::chrono::high_resolution_clock::now());
  }

  uint64_t Microsecond(std::string stop, std::string start) {
    return std::chrono::duration_cast<std::chrono::microseconds>(time_map_.at(stop)-time_map_.at(start)).count();
  }

  uint64_t Second(std::string stop, std::string start) {
    return std::chrono::duration_cast<std::chrono::seconds>(time_map_.at(stop)-time_map_.at(start)).count();
  }

  uint64_t Milliseconds(std::string stop, std::string start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(time_map_.at(stop)-time_map_.at(start)).count();
  }

  void Clear() {
    time_map_.clear();
  }

 private:
  std::map<std::string,std::chrono::_V2::high_resolution_clock::time_point> time_map_;
};

}