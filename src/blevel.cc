#include <iostream>
#include <memory>
#include <vector>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/make_persistent_array_atomic.hpp>
#include "combotree_config.h"
#include "blevel.h"
#include "debug.h"

namespace combotree {

BLevel::BLevel(pmem::obj::pool_base pop, const std::vector<std::pair<uint64_t, uint64_t>>& kv)
    : pop_(pop), in_mem_entry_(nullptr), in_mem_key_(nullptr),
      is_expanding_(false)
{
  assert(kv.size() > 1);
  pmem::obj::pool<Root> pool(pop_);
  root_ = pool.root();
  root_->nr_entry.store(kv[0].first == 0 ? kv.size() : kv.size() + 1);
  root_->size = kv.size();
  root_.persist();
  base_addr_ = (uint64_t)root_.get() - root_.raw().off;
  pmem::obj::make_persistent_atomic<Entry[]>(pop_, root_->entry, root_->nr_entry);
  clevel_slab_ = new Slab<CLevel>(pop_, EntrySize() / 4.0);
  Slab<CLevel::LeafNode>* leaf_slab = new Slab<CLevel::LeafNode>(pop_, EntrySize() / 4.0);
  clevel_mem_ = new CLevel::MemoryManagement(pop, leaf_slab);
  // add one extra lock to make blevel iter's logic simple
  locks_ = new std::shared_mutex[EntrySize() + 1];
  in_mem_entry_ = root_->entry.get();
  assert(GetEntry_(1) == &root_->entry[1]);
  int pos = 0;
  if (kv[0].first != 0) {
    Entry* ent = GetEntry_(pos);
    ent->SetKey(clevel_mem_, 0);
    ent->SetNone(clevel_mem_);
    pos++;
  }
  for (size_t i = 0; i < kv.size(); ++i) {
    std::lock_guard<std::shared_mutex> lock(locks_[pos]);
    Entry* ent = GetEntry_(pos);
    ent->SetKey(clevel_mem_, kv[i].first);
    ent->SetValue(clevel_mem_, kv[i].second);
    pos++;
  }
  in_mem_key_ = new uint64_t[root_->nr_entry];
  for (uint64_t i = 0; i < root_->nr_entry; ++i)
    in_mem_key_[i] = in_mem_entry_[i].key;
}

BLevel::BLevel(pmem::obj::pool_base pop, std::shared_ptr<BLevel> old_blevel)
    : pop_(pop), in_mem_entry_(nullptr), in_mem_key_(nullptr),
      is_expanding_(true)
{
  pmem::obj::pool<Root> pool(pop_);
  root_ = pool.root();
  root_->nr_entry.store(old_blevel->Size() * ENTRY_SIZE_FACTOR);  // reserve some entry for insertion
  root_->size.store(EntrySize());
  root_.persist();
  expanding_entry_index_.store(0, std::memory_order_release);
  base_addr_ = (uint64_t)root_.get() - root_.raw().off;
  pmem::obj::make_persistent_atomic<Entry[]>(pop_, root_->entry, EntrySize());
  clevel_slab_ = new Slab<CLevel>(pop_, EntrySize() / 4.0);
  Slab<CLevel::LeafNode>* leaf_slab = new Slab<CLevel::LeafNode>(pop_, EntrySize());
  clevel_mem_ = new CLevel::MemoryManagement(pop, leaf_slab);
  // add one extra lock to make blevel iter's logic simple
  locks_ = new std::shared_mutex[EntrySize() + 1];
  in_mem_entry_ = root_->entry.get();
  in_mem_key_ = new uint64_t[root_->nr_entry];
  assert(GetEntry_(1) == &root_->entry[1]);
}

void BLevel::ExpandAddEntry_(uint64_t key, uint64_t value, size_t& size) {
  uint64_t entry_index = expanding_entry_index_.load(std::memory_order_acquire);
  if (entry_index < EntrySize()) {
    std::lock_guard<std::shared_mutex> lock(locks_[entry_index]);
    Entry* ent = GetEntry_(entry_index);
    ent->SetKey(clevel_mem_, key);
    ent->SetValue(clevel_mem_, value);
    in_mem_key_[entry_index] = key;
    size++;
    expanding_entry_index_.fetch_add(1, std::memory_order_release);
  } else {
    std::lock_guard<std::shared_mutex> lock(locks_[EntrySize() - 1]);
    Entry* ent = GetEntry_(EntrySize() - 1);
    if (ent->IsClevel()) {
      ent->GetClevel(clevel_mem_)->Insert(clevel_mem_, key, value);
    } else if (ent->IsValue()) {
      CLevel* new_clevel = clevel_slab_->Allocate();
      new_clevel->InitLeaf(clevel_mem_);
      new_clevel->Insert(clevel_mem_, ent->key, ent->GetValue());
      new_clevel->Insert(clevel_mem_, key, value);
      ent->SetCLevel(clevel_mem_, new_clevel);
    } else if (ent->IsNone()) {
      assert(0);
    } else {
      assert(0);
    }
    size++;
  }
}

void BLevel::Expansion(std::shared_ptr<BLevel> old_blevel, std::atomic<uint64_t>& min_key,
                        std::atomic<uint64_t>& max_key) {
  uint64_t old_index = 0;
  expanding_entry_index_.store(0, std::memory_order_release);
  size_t size = 0;
  // handle entry 0 explicit
  {
    std::lock_guard<std::shared_mutex> old_lock(old_blevel->locks_[0]);
    std::lock_guard<std::shared_mutex> lock(locks_[0]);
    min_key.store(0);
    max_key.store(old_blevel->GetKey(1));
    Entry* old_ent = old_blevel->GetEntry_(0);
    Entry* new_ent = GetEntry_(0);
    new_ent->SetKey(clevel_mem_, 0);
    in_mem_key_[0] = 0;
    if (old_ent->IsValue()) {
      new_ent->SetValue(clevel_mem_, old_ent->GetValue());
      size++;
      old_ent->SetInvalid(old_blevel->clevel_mem_);
      old_index++;
    } else if (old_ent->IsClevel()) {
      size_t scan_size = 0;
      CLevel* clevel = old_ent->GetClevel(old_blevel->clevel_mem_);
      clevel->Scan(old_blevel->clevel_mem_, 0, 0, 1, scan_size,
        [&](uint64_t key, uint64_t value) {
          new_ent->SetValue(clevel_mem_, value);
          size++;
        });
      if (scan_size == 1)
        clevel->Delete(old_blevel->clevel_mem_, 0);
      else
        new_ent->SetNone(clevel_mem_);
    } else if (old_ent->IsNone()) {
      new_ent->SetNone(clevel_mem_);
      old_ent->SetInvalid(old_blevel->clevel_mem_);
      old_index++;
    } else if (old_ent->IsInvalid()) {
      assert(0);
    } else {
      assert(0);
    }
    expanding_entry_index_.fetch_add(1, std::memory_order_release);
  }

  // handle entry [1, EntrySize()]
  while (old_index < old_blevel->EntrySize()) {
    std::lock_guard<std::shared_mutex> old_ent_lock(old_blevel->locks_[old_index]);
    min_key.store(old_blevel->GetKey(old_index));
    max_key.store(old_index + 1 == old_blevel->EntrySize() ? UINT64_MAX
                                  : old_blevel->GetKey(old_index + 1));
    Entry* old_ent = old_blevel->GetEntry_(old_index);
    if (old_ent->IsValue()) {
      ExpandAddEntry_(old_ent->key, old_ent->GetValue(), size);
    } else if (old_ent->IsClevel()) {
      size_t scan_size = 0;
      CLevel* clevel = old_ent->GetClevel(old_blevel->clevel_mem_);
      clevel->Scan(old_blevel->clevel_mem_, 0, UINT64_MAX, UINT64_MAX, scan_size,
        [&](uint64_t key, uint64_t value) {
          ExpandAddEntry_(key, value, size);
        });
    } else if (old_ent->IsNone()) {
      ;
    } else if (old_ent->IsInvalid()) {
      assert(0);
    }
    old_ent->SetInvalid(old_blevel->clevel_mem_);
    old_index++;
  }

  if (size > EntrySize())
    root_->size.fetch_add(size - EntrySize());
  else
    root_->size.fetch_sub(EntrySize() - size);
  root_->nr_entry.store(expanding_entry_index_.load(std::memory_order_acquire));
  root_.persist();
  is_expanding_.store(false);
}

BLevel::BLevel(pmem::obj::pool_base pop)
    : pop_(pop), in_mem_entry_(nullptr), in_mem_key_(nullptr),
      is_expanding_(false)
{
  pmem::obj::pool<Root> pool(pop_);
  root_ = pool.root();
  clevel_slab_ = new Slab<CLevel>(pop_, EntrySize() / 4.0);
  Slab<CLevel::LeafNode>* leaf_slab = new Slab<CLevel::LeafNode>(pop_, EntrySize() / 4.0);
  clevel_mem_ = new CLevel::MemoryManagement(pop, leaf_slab);
  locks_ = new std::shared_mutex[EntrySize() + 1];
}

BLevel::~BLevel() {
  if (locks_)
    delete locks_;
  if (in_mem_key_)
    delete in_mem_key_;
  if (clevel_slab_)
    delete clevel_slab_;
}

Status BLevel::Entry::Get(std::shared_mutex* mutex, CLevel::MemoryManagement* mem,
                          uint64_t pkey, uint64_t& pvalue) const {
  std::shared_lock<std::shared_mutex> lock(*mutex);
  if (IsInvalid()) {
    return Status::INVALID;
  } else if (IsValue()) {
    if (pkey == key) {
      pvalue = GetValue();
      return Status::OK;
    } else {
      return Status::DOES_NOT_EXIST;
    }
  } else if (IsClevel()) {
    return GetClevel(mem)->Get(mem, pkey, pvalue);
  } else if (IsNone()) {
    return Status::DOES_NOT_EXIST;
  }
  return Status::INVALID;
}

Status BLevel::Entry::Insert(std::shared_mutex* mutex, CLevel::MemoryManagement* mem,
                             Slab<CLevel>* clevel_slab, uint64_t pkey, uint64_t pvalue) {
  // TODO: lock scope too large. https://stackoverflow.com/a/34995051/7640227
  std::lock_guard<std::shared_mutex> lock(*mutex);
  if (IsInvalid()) {
    return Status::INVALID;
  } else if (IsValue()) {
    if (pkey == key)
      return Status::ALREADY_EXISTS;

    uint64_t old_val = GetValue();
    CLevel* new_clevel = clevel_slab->Allocate();
    new_clevel->InitLeaf(mem);

    [[maybe_unused]] Status debug_status;
    debug_status = new_clevel->Insert(mem, key, old_val);
    assert(debug_status == Status::OK);
    debug_status = new_clevel->Insert(mem, pkey, pvalue);
    assert(debug_status == Status::OK);

    SetCLevel(mem, new_clevel);
    return Status::OK;
  } else if (IsClevel()) {
    return GetClevel(mem)->Insert(mem, pkey, pvalue);
  } else if (IsNone()) {
    if (pkey == key) {
      SetValue(mem, pvalue);
      return Status::OK;
    } else {
      CLevel* new_clevel = clevel_slab->Allocate();
      new_clevel->InitLeaf(mem);

      [[maybe_unused]] Status debug_status;
      debug_status = new_clevel->Insert(mem, pkey, pvalue);
      assert(debug_status == Status::OK);

      SetCLevel(mem, new_clevel);
      return Status::OK;
    }
  }
  return Status::INVALID;
}

Status BLevel::Entry::Update(std::shared_mutex* mutex, CLevel::MemoryManagement* mem,
                             uint64_t pkey, uint64_t pvalue) {
  assert(0);
}

Status BLevel::Entry::Delete(std::shared_mutex* mutex, CLevel::MemoryManagement* mem,
                             uint64_t pkey) {
  std::lock_guard<std::shared_mutex> lock(*mutex);
  if (IsInvalid()) {
    return Status::INVALID;
  } else if (IsValue()) {
    if (pkey == key) {
      SetNone(mem);
      return Status::OK;
    } else {
      return Status::DOES_NOT_EXIST;
    }
  } else if (IsClevel()) {
    return GetClevel(mem)->Delete(mem, pkey);
  } else if (IsNone()) {
    return Status::DOES_NOT_EXIST;
  }
  return Status::INVALID;
}

Status BLevel::Scan(uint64_t min_key, uint64_t max_key,
                    size_t max_size, size_t& size,
                    std::function<void(uint64_t,uint64_t)> callback) {
  if (size >= max_size)
    return Status::OK;

  int end;
  if (is_expanding_.load())
    end = expanding_entry_index_.load(std::memory_order_acquire) - 1;
  else
    end = EntrySize() - 1;
  uint64_t entry_index = Find_(min_key, 0, end);
  {
    std::shared_lock<std::shared_mutex> lock(locks_[entry_index]);
    Entry* ent = GetEntry_(entry_index);
    if (ent->IsInvalid()) {
      return Status::INVALID;
    } else if (ent->IsNone()) {
      ;
    } else if (ent->IsValue()) {
      if (ent->key >= min_key) {
        callback(ent->key, ent->GetValue());
        size++;
      }
    } else if (ent->IsClevel()) {
      CLevel* clevel = ent->GetClevel(clevel_mem_);
      bool finish = clevel->Scan(clevel_mem_, min_key, max_key, max_size, size, callback);
      if (finish)
        return Status::OK;
    }
  }

  entry_index++;
  while (entry_index < EntrySize()) {
    std::shared_lock<std::shared_mutex> lock(locks_[entry_index]);
    Entry* ent = GetEntry_(entry_index);
    if (ent->IsInvalid()) {
      return Status::INVALID;
    } else if (ent->IsNone()) {
      ;
    } else if (ent->IsValue()) {
      if (size >= max_size || ent->key > max_key)
        break;
      callback(ent->key, ent->GetValue());
      size++;
    } else if (ent->IsClevel()) {
      CLevel* clevel = ent->GetClevel(clevel_mem_);
      bool finish = clevel->Scan(clevel_mem_, max_key, max_size, size, callback);
      if (finish)
        return Status::OK;
    }
    entry_index++;
  }
  return Status::OK;
}

uint64_t BLevel::Find_(uint64_t key, uint64_t begin, uint64_t end) const {
  assert(begin < EntrySize());
  assert(end < EntrySize());
  int_fast32_t left = begin;
  int_fast32_t right = end;
  // binary search
  while (left <= right) {
    int middle = (left + right) / 2;
    uint64_t mid_key = GetKey(middle);
    if (mid_key == key) {
      return middle;
    } else if (mid_key < key) {
      left = middle + 1;
    } else {
      right = middle - 1;
    }
  }
  return right;
}

uint64_t BLevel::MinEntryKey() const {
  assert(EntrySize() > 1);
  return GetKey(1);
}

uint64_t BLevel::MaxEntryKey() const {
  return GetKey(EntrySize() - 1);
}

} // namespace combotree