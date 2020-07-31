#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/make_persistent_array_atomic.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include "iterator.h"
#include "blevel.h"
#include "debug.h"

namespace combotree {

BLevel::BLevel(pmem::obj::pool_base& pop, Iterator* iter, uint64_t size)
    : pop_(pop)
{
  pmem::obj::pool<Root> pool(pop);
  CLevel::SetPoolBase(pop);
  root_ = pool.root();
  root_->nr_entry_ = iter->key() == 0 ? size : size + 1;
  root_->size_ = size;
  pmem::obj::make_persistent_atomic<Entry[]>(pop_, root_->entry_, size);
  int pos = 0;
  if (iter->key() != 0)
    GetEntry_(pos++)->key = 0;
  for (size_t i = 0; i < size; ++i) {
    Entry* ent = GetEntry_(pos);
    ent->key = iter->key();
    ent->value = iter->value();
    ent->type = Entry::Type::ENTRY_VALUE;
    iter->Next();
    pos++;
  }
}

BLevel::BLevel(pmem::obj::pool_base& pop)
    : pop_(pop)
{
  pmem::obj::pool<Root> pool(pop);
  root_ = pool.root();
}

bool BLevel::Entry::Get(uint64_t pkey, uint64_t& pvalue) const {
  if (type == Type::ENTRY_VALUE) {
    if (pkey == key) {
      pvalue = value;
      return true;
    } else {
      return false;
    }
  } else if (type == Type::ENTRY_CLVEL) {
    return clevel->Get(pkey, pvalue);
  } else if (type == Type::ENTRY_NONE) {
    return false;
  }
  return false;
}

bool BLevel::Entry::Insert(pmem::obj::pool_base& pop, uint64_t pkey, uint64_t pvalue) {
  if (type == Type::ENTRY_VALUE) {
    if (pkey == key) {
      return false;
    }
    uint64_t old_val = value;
    pmem::obj::make_persistent_atomic<CLevel>(pop, clevel);
    bool res = false;
    res = clevel->Insert(key, old_val);
    assert(res == true);
    res = clevel->Insert(pkey, pvalue);
    assert(res == true);
    type = Type::ENTRY_CLVEL;
    return true;
  } else if (type == Type::ENTRY_CLVEL) {
    return clevel->Insert(pkey, pvalue);
  } else if (type == Type::ENTRY_NONE) {
    if (pkey == key) {
      value = pvalue;
      type = Type::ENTRY_VALUE;
      return true;
    } else {
      pmem::obj::make_persistent_atomic<CLevel>(pop, clevel);
      bool res;
      res = clevel->Insert(pkey, pvalue);
      assert(res == true);
      type = Type::ENTRY_CLVEL;
      return true;
    }
  }
  return false;
}

bool BLevel::Entry::Update(uint64_t pkey, uint64_t pvalue) {
  if (type == Type::ENTRY_VALUE) {
    if (pkey == key) {
      value = pvalue;
      return true;
    } else {
      return false;
    }
  } else if (type == Type::ENTRY_CLVEL) {
    return clevel->Update(pkey, pvalue);
  } else if (type == Type::ENTRY_NONE) {
    return false;
  }
  return false;
}

bool BLevel::Entry::Delete(uint64_t pkey) {
  if (type == Type::ENTRY_VALUE) {
    if (pkey == key) {
      type = Type::ENTRY_NONE;
      return true;
    } else {
      return false;
    }
  } else if (type == Type::ENTRY_CLVEL) {
    return clevel->Delete(pkey);
  } else if (type == Type::ENTRY_NONE) {
    return false;
  }
  return false;
}

uint64_t BLevel::Find_(uint64_t key, uint64_t begin, uint64_t end) const {
  debug_assert(begin >= 0 && begin < EntrySize());
  debug_assert(end >= 0 && end < EntrySize());
  int left = begin;
  int right = end;
  // binary search
  while (left <= right) {
    int middle = (left + right) / 2;
    Entry* entry = GetEntry_(middle);
    if (entry->key == key) {
      return middle;
    } else if (entry->key < key) {
      left = middle + 1;
    } else {
      right = middle - 1;
    }
  }
  return right;
}

uint64_t BLevel::MinKey() const {
  int entry_index = 0;
  while (GetEntry_(entry_index)->type ==
          BLevel::Entry::Type::ENTRY_NONE) {
    entry_index++;
  }
  Entry* ent = GetEntry_(entry_index);
  if (ent->type == BLevel::Entry::Type::ENTRY_CLVEL) {
    auto iter = ent->clevel->begin();
    return iter->key();
  } else if (ent->type == BLevel::Entry::Type::ENTRY_VALUE) {
    return ent->key;
  }
  assert(0);
}

uint64_t BLevel::MaxKey() const {
  int entry_index = EntrySize() - 1;
  while (GetEntry_(entry_index)->type ==
          BLevel::Entry::Type::ENTRY_NONE) {
    entry_index--;
  }
  Entry* ent = GetEntry_(entry_index);
  if (ent->type == BLevel::Entry::Type::ENTRY_CLVEL) {
    auto iter = ent->clevel->end();
    return iter->key();
  } else if (ent->type == BLevel::Entry::Type::ENTRY_VALUE) {
    return ent->key;
  }
  assert(0);
}

uint64_t BLevel::MinEntryKey() const {
  assert(EntrySize() > 1);
  return GetEntry_(1)->key;
}

uint64_t BLevel::MaxEntryKey() const {
  return GetEntry_(EntrySize() - 1)->key;
}

inline Iterator* BLevel::begin() {
  Iterator* iter = new Iter(this);
  iter->SeekToFirst();
  return iter;
}

inline Iterator* BLevel::end() {
  Iterator* iter = new Iter(this);
  iter->SeekToLast();
  return iter;
}

} // namespace combotree