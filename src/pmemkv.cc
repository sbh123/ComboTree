#include <cassert>
#include <filesystem>
// #include <libpmemkv.hpp>
#include "combotree_config.h"
#include "pmemkv.h"

namespace combotree {

std::atomic<bool> PmemKV::read_valid_  = true;
std::atomic<bool> PmemKV::write_valid_ = true;

// using pmem::kv::config;
// using pmem::kv::status;
// using pmem::kv::db;

PmemKV::PmemKV(std::string path, size_t size,
               std::string engine, bool force_create)
    : write_ref_(0), read_ref_(0)
{
  std::filesystem::remove(path);
  // // config cfg;
  // [[maybe_unused]] auto s = cfg.put_string("path", path);
  // assert(s == status::OK);
  // s = cfg.put_uint64("size", size);
  // assert(s == status::OK);
  // s = cfg.put_uint64("force_create", force_create ? 1 : 0);
  // assert(s == status::OK);

  // s = db_->open(engine, std::move(cfg));

  // assert(s == status::OK);
}

namespace {

inline void int2char(uint64_t integer, char* buf) {
  *(uint64_t*)buf = integer;
}

} // anonymous namespace

bool PmemKV::Put(uint64_t key, uint64_t value) {
  bool ret = false;
  WriteRef_();
  if (!write_valid_.load(std::memory_order_acquire))
    return false;
  kv_data.insert(std::make_pair(key, value));
  WriteUnRef_();
  return true;
}

bool PmemKV::Get(uint64_t key, uint64_t& value) const {
  bool ret = false;
  ReadRef_();
  if (!read_valid_.load(std::memory_order_acquire))
    return false;
  if(kv_data.find(key) != kv_data.end()) {
    value = (*(kv_data.find(key))).second;
    ret = true;
  }
  ReadUnRef_();
  return ret;
}

bool PmemKV::Delete(uint64_t key) {
  WriteRef_();
  if (!write_valid_.load(std::memory_order_acquire))
    return false;
  kv_data.erase(key);
  WriteUnRef_();
  return true;
}

size_t PmemKV::Scan(uint64_t min_key, uint64_t max_key, uint64_t max_size,
                    std::vector<std::pair<uint64_t,uint64_t>>& kv) const {
  ReadRef_();
  char key_buf[sizeof(uint64_t)];
  int2char(min_key, key_buf);
  for(auto kv_pair:kv_data) {
    kv.emplace_back(kv_pair);
  }
  ReadUnRef_();
  std::sort(kv.begin(), kv.end());
  if (kv.size() > max_size)
    kv.resize(max_size);
  return kv.size();
}

size_t PmemKV::Scan(uint64_t min_key, uint64_t max_key, uint64_t max_size,
                    void (*callback)(uint64_t,uint64_t,void*), void* arg) const {
  std::vector<std::pair<uint64_t,uint64_t>> kv;
  Scan(min_key, max_key, max_size, kv);
  for (auto& pair : kv)
    callback(pair.first, pair.second, arg);
  return kv.size();
}

} // namespace combotree