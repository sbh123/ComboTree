#pragma once

#include <cassert>
#include <cstdlib>
#include <libpmemkv.hpp>
#include <vector>
#include "iterator.h"

namespace combotree {

using pmem::kv::status;
using pmem::kv::string_view;

namespace {

const uint64_t SIZE = 1024UL * 1024UL * 1024UL; // 1G

} // anonymous namespace

class PmemKV {
 public:
  explicit PmemKV(std::string path, size_t size = SIZE,
                  std::string engine = "cmap", bool force_create = true);

  bool Insert(uint64_t key, uint64_t value) {
    char key_buf[sizeof(uint64_t)];
    char value_buf[sizeof(uint64_t)];
    IntToCharBuf_(key, key_buf);
    IntToCharBuf_(value, value_buf);

    auto s = db_->put(string_view(key_buf, sizeof(uint64_t)),
                      string_view(value_buf, sizeof(uint64_t)));
    return s == status::OK;
  }

  bool Update(uint64_t key, uint64_t value) {
    char key_buf[sizeof(uint64_t)];
    char value_buf[sizeof(uint64_t)];
    IntToCharBuf_(key, key_buf);
    IntToCharBuf_(value, value_buf);

    auto s = db_->put(string_view(key_buf, sizeof(uint64_t)),
                      string_view(value_buf, sizeof(uint64_t)));
    return s == status::OK;
  }

  bool Get(uint64_t key, uint64_t& value) const {
    char key_buf[sizeof(uint64_t)];
    IntToCharBuf_(key, key_buf);

    auto s = db_->get(string_view(key_buf, sizeof(uint64_t)),
        [&](string_view value_str){ value = *(uint64_t*)value_str.data(); });
    return s == status::OK;
  }

  bool Delete(uint64_t key) {
    char key_buf[sizeof(uint64_t)];
    IntToCharBuf_(key, key_buf);
    auto s = db_->remove(string_view(key_buf, sizeof(uint64_t)));
    return s == status::OK;
  }

  size_t Size() const {
    size_t size;
    auto s = db_->count_all(size);
    assert(s == status::OK);
    return size;
  }

  class Iter;

  Iterator* begin();
  Iterator* end();

 private:
  pmem::kv::db* db_;

  void IntToCharBuf_(uint64_t integer, char* buf) const {
    *(uint64_t*)buf = integer;
  }
};

class PmemKV::Iter : public Iterator {
 public:
  Iter(PmemKV* pmemkv) : pmemkv_(pmemkv), size_(0), index_(0)
  {
    auto s =pmemkv_->db_->get_all(
      [&](string_view key_str, string_view value_str) {
        uint64_t key = *(uint64_t*)key_str.data();
        uint64_t value = *(uint64_t*)value_str.data();
        kv_pair_.emplace_back(key, value);
        size_++;
        return 0;
      });
    assert(s == status::OK);
  }

  ~Iter() {};

  bool Begin() const { return index_ == 0; }

  bool End() const { return (size_t)index_ == size_ - 1; }

  void SeekToFirst() { index_ = 0; }

  void SeekToLast() { index_ = size_ - 1; }

  void Seek(uint64_t target) {
    assert(0);
  }

  void Next() { index_++; }

  void Prev() { index_--; }

  uint64_t key() const {
    return kv_pair_[index_].first;
  }

  uint64_t value() const {
    return kv_pair_[index_].second;
  }

 private:
  PmemKV* pmemkv_;
  size_t size_;
  int index_;
  std::vector<std::pair<uint64_t, uint64_t>> kv_pair_;
};

} // namespace combotree