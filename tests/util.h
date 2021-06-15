#pragma once

#include <algorithm>
#include <chrono>
#include <iostream>
#include <functional>
#include <fstream>
#include <thread>
#include <vector>
#include <cassert>
#include <unordered_map>

#define ROW_WIDTH 1

//#define PRINT_ERRORS

enum DataType {
  UINT32 = 0,
  UINT64 = 1
};

template<class KeyType>
struct KeyValue {
  KeyType key;
  uint64_t value;
} __attribute__((packed));

template<class KeyType>
struct Row {
  KeyType key;
  uint64_t data[ROW_WIDTH];
};

template<class KeyType = uint64_t>
struct EqualityLookup {
  KeyType key;
  uint64_t result;
};

struct SearchBound {
  size_t start;
  size_t stop;
};

namespace util {

const static uint64_t NOT_FOUND = std::numeric_limits<uint64_t>::max();


static void fail(const std::string& message) {
  std::cerr << message << std::endl;
  exit(EXIT_FAILURE);
}

[[maybe_unused]]
static std::string get_suffix(const std::string& filename) {
  const std::size_t pos = filename.find_last_of("_");
  if (pos==filename.size() - 1 || pos==std::string::npos)
    return "";
  return filename.substr(pos + 1);
}

[[maybe_unused]]
static DataType resolve_type(const std::string& filename) {
  const std::string suffix = util::get_suffix(filename);
  if (suffix=="uint32") {
    return DataType::UINT32;
  } else if (suffix=="uint64") {
    return DataType::UINT64;
  } else {
    std::cerr << "type " << suffix << " not supported" << std::endl;
    exit(EXIT_FAILURE);
  }
}

// Pins the current thread to core `core_id`.
static void set_cpu_affinity(const uint32_t core_id) __attribute__((unused));
static void set_cpu_affinity(const uint32_t core_id) {
#ifdef __linux__
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(core_id % std::thread::hardware_concurrency(), &mask);
  const int result = pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask);
  if (result != 0)
    fail("failed to set CPU affinity");
#else
  (void) core_id;
  std::cout << "we only support thread pinning under Linux" << std::endl;
#endif
}

static uint64_t timing(std::function<void()> fn) {
  const auto start = std::chrono::high_resolution_clock::now();
  fn();
  const auto end = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
      end - start).count();
}

// Checks whether data is duplicate free.
// Note that data has to be sorted.
template<typename T>
static bool is_unique(const std::vector<T>& data) {
  for (size_t i = 1; i < data.size(); ++i) {
    if (data[i]==data[i - 1])
      return false;
  }
  return true;
}

template<class KeyType>
static bool is_unique(const std::vector<KeyValue<KeyType>>& data) {
  for (size_t i = 1; i < data.size(); ++i) {
    if (data[i].key==data[i - 1].key)
      return false;
  }
  return true;
}

// Loads values from binary file into vector.
template<typename T>
static std::vector<T> load_data(const std::string& filename,
                                size_t max_size = 1e10,
                                bool print = true) {
  std::vector<T> data;
  const uint64_t ns = util::timing([&] {
    std::ifstream in(filename, std::ios::binary);
    if (!in.is_open()) {
      std::cerr << "unable to open " << filename << std::endl;
      exit(EXIT_FAILURE);
    }
    // Read size.
    uint64_t size;
    in.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    std::cerr << "Data size: " << size << std::endl;
    size = std::min(size, max_size);
    data.resize(size);

    // Read values.
    in.read(reinterpret_cast<char*>(data.data()), size*sizeof(T));
    in.close();
  });
  const uint64_t ms = ns/1e6;

  if (print) {
    std::cout << "read " << data.size() << " values from " << filename << " in "
              << ms << " ms (" << static_cast<double>(data.size())/1000/ms
              << " M values/s)" << std::endl;
  }

  return data;
}

// Writes values from vector into binary file.
template<typename T>
static void write_data(const std::vector<T>& data,
                       const std::string& filename,
                       const bool print = true) {
  const uint64_t ns = util::timing([&] {
    std::ofstream out(filename, std::ios_base::trunc | std::ios::binary);
    if (!out.is_open()) {
      std::cerr << "unable to open " << filename << std::endl;
      exit(EXIT_FAILURE);
    }
    // Write size.
    const uint64_t size = data.size();
    out.write(reinterpret_cast<const char*>(&size), sizeof(uint64_t));
    // Write values.
    out.write(reinterpret_cast<const char*>(data.data()), size*sizeof(T));
    out.close();
  });
  const uint64_t ms = ns/1e6;
  if (print) {
    std::cout << "wrote " << data.size() << " values to " << filename << " in "
              << ms << " ms (" << static_cast<double>(data.size())/1000/ms
              << " M values/s)" << std::endl;
  }
}

// Returns a duplicate-free copy.
// Note that data has to be sorted.
template<typename T>
static std::vector<T> remove_duplicates(const std::vector<T>& data) {
  std::vector<T> result = data;
  auto last = std::unique(result.begin(), result.end());
  result.erase(last, result.end());
  return result;
}

// Returns a value for a key at position i.
template<class KeyType>
static uint64_t get_value(const KeyType i) {
  return i;
}

// Generates deterministic values for keys.
template<class KeyType>
static std::vector<Row<KeyType>> add_values(const std::vector<KeyType>& keys) {
  std::vector<Row<KeyType>> result;
  result.reserve(keys.size());
  
  for (uint64_t i = 0; i < keys.size(); ++i) {
    Row<KeyType> row;
    row.key = keys[i];
    for (int j = 0; j < ROW_WIDTH; j++) {
      row.data[j] = get_value(i*(j+1));
    }
        
    result.push_back(row);
  }
  return result;
}


// Based on: https://en.wikipedia.org/wiki/Xorshift
class FastRandom {
 public:
  explicit FastRandom(uint64_t seed = 2305843008139952128ull) // The 8th perfect number found 1772 by Euler with <3
      : seed(seed) {}
  uint32_t RandUint32() {
    seed ^= (seed << 13);
    seed ^= (seed >> 15);
    return (uint32_t) (seed ^= (seed << 5));
  }
  int32_t RandInt32() { return (int32_t) RandUint32(); }
  uint32_t RandUint32(uint32_t inclusive_min, uint32_t inclusive_max) {
    return inclusive_min + RandUint32()%(inclusive_max - inclusive_min + 1);
  }
  int32_t RandInt32(int32_t inclusive_min, int32_t inclusive_max) {
    return inclusive_min + RandUint32()%(inclusive_max - inclusive_min + 1);
  }
  float RandFloat(float inclusive_min, float inclusive_max) {
    return inclusive_min + ScaleFactor()*(inclusive_max - inclusive_min);
  }
  // returns float between 0 and 1
  float ScaleFactor() {
    return static_cast<float>(RandUint32())
        /std::numeric_limits<uint32_t>::max();
  }
  bool RandBool() { return RandUint32()%2==0; }

  uint64_t seed;

  static constexpr uint64_t Min() { return 0; }
  static constexpr uint64_t Max() { return std::numeric_limits<uint64_t>::max(); }
};

uint64_t get_cellid(const std::string &line) {
  uint64_t id;
  double lat, lon;
  std::stringstream strin(line);
          strin >> id >> lon >> lat;
  return id;
}

double get_longitude(const std::string &line) {
  uint64_t id;
  double lat, lon;
  std::stringstream strin(line);
          strin >> id >> lon >> lat;
  return lon;
}

double get_lat(const std::string &line) {
  uint64_t id;
  double lat, lon;
  std::stringstream strin(line);
          strin >> id >> lon >> lat;
  return lat;
}

uint64_t get_longlat(const std::string &line) {
  uint64_t id;
  double lat, lon;
  std::stringstream strin(line);
          strin >> id >> lon >> lat;
  return (lon * 180 + lat) * 1e7;
}

template<typename T>
std::vector<T>read_data_from_osm(const std::string load_file, 
    T (*get_data)(const std::string &) = []{ return static_cast<T>(0);},
    const std::string output = "/home/sbh/generate_random_osm_longlat.dat")
{
  std::vector<T> data;
  std::set<T> unique_keys;
  std::cout << "Use: " << __FUNCTION__ << std::endl;
    const uint64_t ns = util::timing([&] { 
      std::ifstream in(load_file);
      if (!in.is_open()) {
        std::cerr << "unable to open " << load_file << std::endl;
        exit(EXIT_FAILURE);
      }
      uint64_t id, size = 0;
      double lat, lon;
      while (!in.eof())
      {
        /* code */
        std::string tmp;
        getline(in, tmp); // 去除第一行
        while(getline(in, tmp)) {
          T key = get_data(tmp);
          unique_keys.insert(key);
          size ++;
          if(size % 100000 == 0) std::cerr << "Load: " << size << "\r";
        }
      }
      in.close();
      std::cerr << "Finshed loads ......\n";
      data.assign(unique_keys.begin(), unique_keys.end());
      std::random_shuffle(data.begin(), data.end());
      size = data.size();
      std::cerr << "Finshed random ......\n"; 
      std::ofstream out(output, std::ios::binary);
      out.write(reinterpret_cast<char*>(&size), sizeof(uint64_t));
      out.write(reinterpret_cast<char*>(data.data()), data.size() * sizeof(uint64_t));
      out.close(); 
      std::cout << "read size: " << size << ", unique data: " << unique_keys.size() << std::endl;
  });
  const uint64_t ms = ns/1e6;
  std::cout << "generate " << data.size() << " values in "
            << ms << " ms (" << static_cast<double>(data.size())/1000/ms
            << " M values/s)" << std::endl;   
  return data;
}

template<typename T>
std::vector<T>load_data_from_osm(
    const std::string dataname = "/home/sbh/generate_random_osm_cellid.dat")
{
  return util::load_data<T>(dataname);
}

std::vector<uint64_t> generate_random_ycsb(size_t op_num)
{
  std::vector<uint64_t> data; 
  data.resize(op_num);
  std::cout << "Use: " << __FUNCTION__ << std::endl;
  const uint64_t ns = util::timing([&] { 
    combotree::Random rnd(0, op_num - 1);
    for (size_t i = 0; i < op_num; ++i)
      data[i] = utils::Hash(i);
    // for (size_t i = 0; i < op_num; ++i)
    //   std::swap(data[i], data[rnd.Next()]);
  });
  const uint64_t ms = ns/1e6;
  std::cout << "generate " << data.size() << " values in "
            << ms << " ms (" << static_cast<double>(data.size())/1000/ms
            << " M values/s)" << std::endl;   
  return data;
}

std::vector<uint64_t> generate_uniform_random(size_t op_num)
{
  std::vector<uint64_t> data; 
  data.resize(op_num);
  std::cout << "Use: " << __FUNCTION__ << std::endl;
  const uint64_t ns = util::timing([&] { 
    combotree::Random rnd(0, UINT64_MAX);
    for (size_t i = 0; i < op_num; ++i)
      data[i] = rnd.Next();
  });
  const uint64_t ms = ns/1e6;
  std::cout << "generate " << data.size() << " values in "
            << ms << " ms (" << static_cast<double>(data.size())/1000/ms
            << " M values/s)" << std::endl;  
  return data;
}

} // namespace util
