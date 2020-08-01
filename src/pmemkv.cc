#include <cassert>
#include <filesystem>
#include <libpmemkv.hpp>
#include "pmemkv.h"

namespace combotree {

using pmem::kv::config;
using pmem::kv::status;
using pmem::kv::db;

PmemKV::PmemKV(std::string path, size_t size,
               std::string engine, bool force_create)
    : db_(new db()), write_ref_(0), read_ref_(0)
{
  std::filesystem::remove(path);
  config cfg;
  auto s = cfg.put_string("path", path);
  assert(s == status::OK);
  s = cfg.put_uint64("size", size);
  assert(s == status::OK);
  s = cfg.put_uint64("force_create", force_create ? 1 : 0);
  assert(s == status::OK);

  s = db_->open(engine, std::move(cfg));
  assert(s == status::OK);
}

Iterator* PmemKV::begin() {
  Iterator* iter = new Iter(this);
  iter->SeekToFirst();
  return iter;
}

Iterator* PmemKV::end() {
  Iterator* iter = new Iter(this);
  iter->SeekToLast();
  return iter;
}

} // namespace combotree