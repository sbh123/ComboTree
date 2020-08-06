#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/make_persistent_array_atomic.hpp>
#include <iostream>
#include "blevel.h"
#include "debug.h"

namespace combotree {

BLevel::BLevel(pmem::obj::pool_base pop, Iterator* iter, uint64_t size)
    : pop_(pop)
{
  pmem::obj::pool<Root> pool(pop_);
  // FIXME: bug! when combotree expanding, both old and new pool can be inserted.
  CLevel::SetPoolBase(pop_);
  root_ = pool.root();
  root_->nr_entry = iter->key() == 0 ? size : size + 1;
  root_->size = size;
  base_addr_ = (uint64_t)root_.get() - root_.raw().off;
  pmem::obj::make_persistent_atomic<Entry[]>(pop_, root_->entry, root_->nr_entry);
  // add one extra lock to make blevel iter's logic simple
  locks_ = new std::shared_mutex[root_->nr_entry + 1];
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
  for (int i = 0; i < root_->nr_entry; ++i) {
    in_mem_key_[i] = in_mem_entry_[i].key;
  }
}

BLevel::BLevel(pmem::obj::pool_base pop)
    : pop_(pop)
{
  pmem::obj::pool<Root> pool(pop_);
  root_ = pool.root();
  locks_ = new std::shared_mutex[root_->nr_entry];
}

BLevel::~BLevel() {
  if (locks_)
    delete locks_;
}

bool BLevel::Entry::Get(std::shared_mutex* mutex, uint64_t base_addr, uint64_t pkey, uint64_t& pvalue) const {
  // std::shared_lock<std::shared_mutex> lock(*mutex);
  if (IsValue()) {
    if (pkey == key) {
      pvalue = GetValue();
      return true;
    } else {
      return false;
    }
  } else if (IsClevel()) {
    return GetClevel(base_addr)->Get(pkey, pvalue);
  } else if (IsNone()) {
    return false;
  }
  return false;
}

bool BLevel::Entry::Insert(std::shared_mutex* mutex, uint64_t base_addr, pmem::obj::pool_base& pop, uint64_t pkey, uint64_t pvalue) {
  // TODO: lock scope too large. https://stackoverflow.com/a/34995051/7640227
  // std::lock_guard<std::shared_mutex> lock(*mutex);
  if (IsValue()) {
    if (pkey == key) {
      return false;
    }
    uint64_t old_val = GetValue();
    pmem::obj::persistent_ptr<CLevel> new_clevel = nullptr;
    pmem::obj::make_persistent_atomic<CLevel>(pop, new_clevel);
    new_clevel->InitLeaf();

    bool res = false;
    res = new_clevel->Insert(key, old_val);
    assert(res == true);
    res = new_clevel->Insert(pkey, pvalue);
    assert(res == true);
    SetTypeClevel();
    SetClevel(new_clevel.get(), base_addr);
    return true;
  } else if (IsClevel()) {
    return GetClevel(base_addr)->Insert(pkey, pvalue);
  } else if (IsNone()) {
    if (pkey == key) {
      SetValue(pvalue);
      SetTypeValue();
      return true;
    } else {
      pmem::obj::persistent_ptr<CLevel> new_clevel;
      pmem::obj::make_persistent_atomic<CLevel>(pop, new_clevel);
      new_clevel->InitLeaf();

      bool res;
      res = new_clevel->Insert(pkey, pvalue);
      assert(res == true);
      SetTypeClevel();
      SetClevel(new_clevel.get(), base_addr);
      return true;
    }
  }
  return false;
}

bool BLevel::Entry::Update(std::shared_mutex* mutex, uint64_t base_addr, uint64_t pkey, uint64_t pvalue) {
  // std::lock_guard<std::shared_mutex> lock(*mutex);
  if (IsValue()) {
    if (pkey == key) {
      value = GetValue();
      return true;
    } else {
      return false;
    }
  } else if (IsClevel()) {
    return GetClevel(base_addr)->Update(pkey, pvalue);
  } else if (IsNone()) {
    return false;
  }
  return false;
}

bool BLevel::Entry::Delete(std::shared_mutex* mutex, uint64_t base_addr, uint64_t pkey) {
  // std::lock_guard<std::shared_mutex> lock(*mutex);
  if (IsValue()) {
    if (pkey == key) {
      SetTypeNone();
      return true;
    } else {
      return false;
    }
  } else if (IsClevel()) {
    return GetClevel(base_addr)->Delete(pkey);
  } else if (IsNone()) {
    return false;
  }
  return false;
}

uint64_t BLevel::Find_(uint64_t key, uint64_t begin, uint64_t end) const {
  assert(begin >= 0 && begin < EntrySize());
  assert(end >= 0 && end < EntrySize());
  // if (end - begin > 10)
  //   std::cout << end - begin << std::endl;
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