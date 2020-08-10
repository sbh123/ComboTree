#include <memory>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/make_persistent_array_atomic.hpp>
#include "combotree_config.h"
#include "blevel.h"
#include "debug.h"

namespace combotree {

BLevel::BLevel(pmem::obj::pool_base pop, Iterator* iter, uint64_t size)
    : pop_(pop), in_mem_entry_(nullptr), in_mem_key_(nullptr),
      is_expanding_(false)
{
  pmem::obj::pool<Root> pool(pop_);
  root_ = pool.root();
  root_->nr_entry = iter->key() == 0 ? size : size + 1;
  root_->size = size;
  base_addr_ = (uint64_t)root_.get() - root_.raw().off;
  pmem::obj::make_persistent_atomic<Entry[]>(pop_, root_->entry, root_->nr_entry);
  // add one extra lock to make blevel iter's logic simple
  locks_ = new std::shared_mutex[EntrySize() + 1];
  in_mem_entry_ = root_->entry.get();
  assert(GetEntry_(1) == &root_->entry[1]);
  int pos = 0;
  if (iter->key() != 0) {
    Entry* ent = GetEntry_(pos++);
    ent->key = 0;
    ent->SetTypeNone();
  }
  for (size_t i = 0; i < size; ++i) {
    std::lock_guard<std::shared_mutex> lock(locks_[pos]);
    Entry* ent = GetEntry_(pos);
    ent->key = iter->key();
    ent->SetTypeValue();
    ent->SetValue(iter->value());
    iter->Next();
    pos++;
  }
  assert(iter->End());
  in_mem_key_ = new uint64_t[root_->nr_entry];
  for (uint64_t i = 0; i < root_->nr_entry; ++i) {
    in_mem_key_[i] = in_mem_entry_[i].key;
  }
}

BLevel::BLevel(pmem::obj::pool_base pop, std::shared_ptr<BLevel> old_blevel)
    : pop_(pop), in_mem_entry_(nullptr), in_mem_key_(nullptr),
      is_expanding_(true)
{
  pmem::obj::pool<Root> pool(pop_);
  root_ = pool.root();
  root_->nr_entry = old_blevel->Size() * ENTRY_SIZE_FACTOR;  // reserve some entry for insertion
  root_->size = 0;
  base_addr_ = (uint64_t)root_.get() - root_.raw().off;
  pmem::obj::make_persistent_atomic<Entry[]>(pop_, root_->entry, EntrySize());
  // add one extra lock to make blevel iter's logic simple
  locks_ = new std::shared_mutex[EntrySize() + 1];
  in_mem_entry_ = root_->entry.get();
  in_mem_key_ = new uint64_t[root_->nr_entry];
  assert(GetEntry_(1) == &root_->entry[1]);
}

void BLevel::ExpandAddEntry_(uint64_t& index, uint64_t key, uint64_t value) {
  if (index < EntrySize()) {
    std::lock_guard<std::shared_mutex> lock(locks_[index]);
    Entry* ent = GetEntry_(index);
    ent->SetTypeValue();
    ent->SetKey(key);
    in_mem_key_[index] = key;
    ent->SetValue(value);
    index++;
    root_->size++;
  } else {
    std::lock_guard<std::shared_mutex> lock(locks_[EntrySize() - 1]);
    Entry* ent = GetEntry_(EntrySize() - 1);
    if (ent->IsClevel()) {
      ent->GetClevel(base_addr_)->Insert(pop_, key, value);
    } else if (ent->IsValue()) {
      pmem::obj::persistent_ptr<CLevel> new_clevel;
      pmem::obj::make_persistent_atomic<CLevel>(pop_, new_clevel);
      new_clevel->InitLeaf(pop_);
      new_clevel->Insert(pop_, ent->GetKey(), ent->GetValue());
      new_clevel->Insert(pop_, key, value);
      ent->SetTypeClevel();
      ent->SetClevel(new_clevel.get(), base_addr_);
    } else if (ent->IsNone()) {
      // ent->SetTypeValue();
      // ent->SetKey(key);
      // in_mem_key_[EntrySize() - 1] = key;
      // ent->SetValue(value);
      assert(0);
    } else {
      assert(0);
    }
    root_->size++;
  }
}

void BLevel::Expansion(std::shared_ptr<BLevel> old_blevel, std::atomic<uint64_t>& min_key,
                        std::atomic<uint64_t>& max_key) {
  uint64_t old_index = 0;
  uint64_t new_index = 0;
  // handle entry 0 explicit
  old_blevel->locks_[0].lock();
  locks_[0].lock();
  min_key.store(0);
  max_key.store(old_blevel->GetKey(1));
  Entry* old_ent = old_blevel->GetEntry_(0);
  Entry* new_ent = GetEntry_(0);
  new_ent->SetKey(0);
  in_mem_key_[0] = 0;
  if (old_ent->IsValue()) {
    new_ent->SetTypeValue();
    new_ent->SetValue(old_ent->GetValue());
    root_->size++;
    old_ent->SetTypeUnValid();
    old_index++;
  } else if (old_ent->IsClevel()) {
    Iterator* clevel_iter = old_ent->GetClevel(old_blevel->base_addr_)->begin();
    if (!clevel_iter->End() && clevel_iter->key() == 0) {
      new_ent->SetTypeValue();
      new_ent->SetValue(clevel_iter->value());
      root_->size++;
      delete clevel_iter;
      old_ent->GetClevel(old_blevel->base_addr_)->Delete(0);
    } else {
      delete clevel_iter;
    }
  } else if (old_ent->IsNone()) {
    new_ent->SetTypeNone();
    old_ent->SetTypeUnValid();
    old_index++;
  } else if (old_ent->IsUnValid()) {
    assert(0);
  }
  new_index++;
  locks_[0].unlock();
  old_blevel->locks_[0].unlock();

  // handle entry [1, EntrySize()]
  while (old_index < old_blevel->EntrySize()) {
    std::lock_guard<std::shared_mutex> old_ent_lock(old_blevel->locks_[old_index]);
    max_key.store(old_index + 1 == old_blevel->EntrySize() ? UINT64_MAX
                                  : old_blevel->GetKey(old_index + 1));
    Entry* old_ent = old_blevel->GetEntry_(old_index);
    if (old_ent->IsValue()) {
      ExpandAddEntry_(new_index, old_ent->GetKey(), old_ent->GetValue());
    } else if (old_ent->IsClevel()) {
      Iterator* clevel_iter = old_ent->GetClevel(old_blevel->base_addr_)->begin();
      while (!clevel_iter->End()) {
        ExpandAddEntry_(new_index, clevel_iter->key(), clevel_iter->value());
        clevel_iter->Next();
      }
      delete clevel_iter;
    } else if (old_ent->IsNone()) {
      ;
    } else if (old_ent->IsUnValid()) {
      assert(0);
    }
    old_ent->SetTypeUnValid();
    old_index++;
    min_key.store(old_index == old_blevel->EntrySize() ? UINT64_MAX
                                  : old_blevel->GetKey(old_index));
  }

  root_->nr_entry = new_index;
  is_expanding_.store(false);
}

BLevel::BLevel(pmem::obj::pool_base pop)
    : pop_(pop), in_mem_entry_(nullptr), in_mem_key_(nullptr),
      is_expanding_(false)
{
  pmem::obj::pool<Root> pool(pop_);
  root_ = pool.root();
  locks_ = new std::shared_mutex[EntrySize() + 1];
}

BLevel::~BLevel() {
  if (locks_)
    delete locks_;
  if (in_mem_key_)
    delete in_mem_key_;
}

Status BLevel::Entry::Get(std::shared_mutex* mutex, uint64_t base_addr, uint64_t pkey, uint64_t& pvalue) const {
  std::shared_lock<std::shared_mutex> lock(*mutex);
  if (IsUnValid()) {
    return Status::UNVALID;
  } else if (IsValue()) {
    if (pkey == key) {
      pvalue = GetValue();
      return Status::OK;
    } else {
      return Status::DOES_NOT_EXIST;
    }
  } else if (IsClevel()) {
    return GetClevel(base_addr)->Get(pkey, pvalue);
  } else if (IsNone()) {
    return Status::DOES_NOT_EXIST;
  }
  return Status::UNVALID;
}

Status BLevel::Entry::Insert(std::shared_mutex* mutex, uint64_t base_addr, pmem::obj::pool_base& pop, uint64_t pkey, uint64_t pvalue) {
  // TODO: lock scope too large. https://stackoverflow.com/a/34995051/7640227
  std::lock_guard<std::shared_mutex> lock(*mutex);
  if (IsUnValid()) {
    return Status::UNVALID;
  } else if (IsValue()) {
    if (pkey == key) {
      return Status::ALREADY_EXISTS;
    }
    uint64_t old_val = GetValue();
    pmem::obj::persistent_ptr<CLevel> new_clevel = nullptr;
    pmem::obj::make_persistent_atomic<CLevel>(pop, new_clevel);
    new_clevel->InitLeaf(pop);

    [[maybe_unused]] Status s;
    s = new_clevel->Insert(pop, key, old_val);
    assert(s == Status::OK);
    s = new_clevel->Insert(pop, pkey, pvalue);
    assert(s == Status::OK);
    SetTypeClevel();
    SetClevel(new_clevel.get(), base_addr);
    return Status::OK;
  } else if (IsClevel()) {
    return GetClevel(base_addr)->Insert(pop, pkey, pvalue);
  } else if (IsNone()) {
    if (pkey == key) {
      SetValue(pvalue);
      SetTypeValue();
      return Status::OK;
    } else {
      pmem::obj::persistent_ptr<CLevel> new_clevel;
      pmem::obj::make_persistent_atomic<CLevel>(pop, new_clevel);
      new_clevel->InitLeaf(pop);

      [[maybe_unused]] Status s;
      s = new_clevel->Insert(pop, pkey, pvalue);
      assert(s == Status::OK);
      SetTypeClevel();
      SetClevel(new_clevel.get(), base_addr);
      return Status::OK;
    }
  }
  return Status::UNVALID;
}

Status BLevel::Entry::Update(std::shared_mutex* mutex, uint64_t base_addr, uint64_t pkey, uint64_t pvalue) {
  assert(0);
}

Status BLevel::Entry::Delete(std::shared_mutex* mutex, uint64_t base_addr, uint64_t pkey) {
  std::lock_guard<std::shared_mutex> lock(*mutex);
  if (IsUnValid()) {
    return Status::UNVALID;
  } else if (IsValue()) {
    if (pkey == key) {
      SetTypeNone();
      return Status::OK;
    } else {
      return Status::DOES_NOT_EXIST;
    }
  } else if (IsClevel()) {
    return GetClevel(base_addr)->Delete(pkey);
  } else if (IsNone()) {
    return Status::DOES_NOT_EXIST;
  }
  return Status::UNVALID;
}

uint64_t BLevel::Find_(uint64_t key, uint64_t begin, uint64_t end) const {
  assert(begin >= 0 && begin < EntrySize());
  assert(end >= 0 && end < EntrySize());
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

uint64_t BLevel::MinKey() const {
  int entry_index = 0;
  while (GetEntry_(entry_index)->IsNone()) {
    entry_index++;
  }
  Entry* ent = GetEntry_(entry_index);
  if (ent->IsClevel()) {
    auto iter = ent->GetClevel(base_addr_)->begin();
    return iter->key();
  } else if (ent->IsValue()) {
    return ent->key;
  }
  assert(0);
  return 0;
}

uint64_t BLevel::MaxKey() const {
  int entry_index = EntrySize() - 1;
  while (GetEntry_(entry_index)->IsNone()) {
    entry_index--;
  }
  Entry* ent = GetEntry_(entry_index);
  if (ent->IsClevel()) {
    auto iter = ent->GetClevel(base_addr_)->end();
    return iter->key();
  } else if (ent->IsValue()) {
    return ent->key;
  }
  assert(0);
  return 0;
}

uint64_t BLevel::MinEntryKey() const {
  assert(EntrySize() > 1);
  return GetKey(1);
}

uint64_t BLevel::MaxEntryKey() const {
  return GetKey(EntrySize() - 1);
}

Iterator* BLevel::begin() {
  Iterator* iter = new Iter(this);
  iter->SeekToFirst();
  return iter;
}

Iterator* BLevel::end() {
  Iterator* iter = new Iter(this);
  iter->SeekToLast();
  return iter;
}

} // namespace combotree