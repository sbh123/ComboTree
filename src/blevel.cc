#include <cstring>
#include <cassert>
#include "blevel.h"
#include "config.h"

namespace combotree {

namespace { // anonymous namespace

uint64_t prefix_mask[9] = {
  0x0000000000000000UL,
  0xFF00000000000000UL,
  0xFFFF000000000000UL,
  0xFFFFFF0000000000UL,
  0xFFFFFFFF00000000UL,
  0xFFFFFFFFFF000000UL,
  0xFFFFFFFFFFFF0000UL,
  0xFFFFFFFFFFFFFF00UL,
  0xFFFFFFFFFFFFFFFFUL
};

uint64_t suffix_mask[9] = {
  0x0000000000000000UL,
  0x00000000000000FFUL,
  0x000000000000FFFFUL,
  0x0000000000FFFFFFUL,
  0x00000000FFFFFFFFUL,
  0x000000FFFFFFFFFFUL,
  0x0000FFFFFFFFFFFFUL,
  0x00FFFFFFFFFFFFFFUL,
  0xFFFFFFFFFFFFFFFFUL,
};

// require: little endian
uint64_t CommonPrefixBytes(uint64_t a, uint64_t b) {
  uint64_t diff = a ^ b;
  for (int i = 0; i < 8; ++i)
    if (((char*)&diff)[i] == 0)
      return 8 - i;
  return 0;
}

} // anonymous namespace


/************************** BLevel::Entry ***************************/
BLevel::Entry::Entry(uint64_t key, uint64_t value, int prefix_len)
  : entry_key(key), prefix_bytes(prefix_len),
    suffix_bytes(8-prefix_len), buf_entries(0)
{
  CalcMaxEntries_();
  Put(key, value);
}

uint64_t BLevel::Entry::KeyAt_(int index) const {
  uint64_t result = entry_key&prefix_mask[prefix_bytes];
  // unaligned 8-byte read
  result |= *(uint64_t*)(buf+suffix_bytes*index) & suffix_mask[suffix_bytes];
  return result;
}

uint8_t* BLevel::Entry::key_(int index) const {
  // discard const
  return (uint8_t*)&buf[suffix_bytes*index];
}

uint64_t* BLevel::Entry::value_(int index) const {
  return (uint64_t*)&buf[sizeof(buf)-(index+1)*8];
}

uint64_t BLevel::Entry::Key(int index) const {
  return KeyAt_(index);
}

uint64_t BLevel::Entry::Value(int index) const {
  return *value_(index);
}

CLevel* BLevel::Entry::clevel_() const {
  return (CLevel*)Config::PmemAddr(clevel);
}

void BLevel::Entry::SetKey_(int index, uint64_t key) {
  // little endian
  memcpy(key_(index), &key, suffix_bytes);
}

int BLevel::Entry::BinarySearch_(uint64_t key, bool& find) const {
  int left = 0;
  int right = buf_entries - 1;
  while (left <= right) {
    int middle = (left + right) / 2;
    uint64_t mid_key = KeyAt_(middle);
    if (mid_key == key) {
      find = true;
      return middle;
    } else if (mid_key > key) {
      right = middle - 1;
    } else {
      left = middle + 1;
    }
  }
  find = false;
  return left;
}

bool BLevel::Entry::Put(uint64_t key, uint64_t value) {
  assert((key&prefix_mask[prefix_bytes]) == (entry_key&prefix_mask[prefix_bytes]));
  // buf full, write to clevel
  if (buf_entries == max_entries) {
    // TODO: first clear buf to non-block another write?
    WriteToCLevel_();
    ClearBuf_();
  }

  bool exist;
  int index = BinarySearch_(key, exist);
  // already in, update
  if (exist) {
    *value_(index) = value;
    flush(value_(index));
    fence();
    return true;
  }
  // not exist
  if (index != buf_entries) {
    // move key
    memmove(key_(index+1), key_(index), (buf_entries-index)*suffix_bytes);
    // move value
    memmove(value_(buf_entries), value_(buf_entries-1), (buf_entries-index)*8);
  }
  SetKey_(index, key);
  *value_(index) = value;
  buf_entries++;

  flush(&meta);
  flush(value_(index));
  fence();
  return true;
};

bool BLevel::Entry::Get(uint64_t key, uint64_t& value) const {
  bool exist;
  int index = BinarySearch_(key, exist);
  if (exist) {
    value = *value_(index);
    return true;
  }
  if (clevel) {
    assert(0);
    // return clevel_()->Get(key, value) == Status::OK;
  } else {
    return false;
  }
}

bool BLevel::Entry::Delete(uint64_t key, uint64_t* value) {
  bool exist;
  int index = BinarySearch_(key, exist);
  if (!exist)
    return false;
  if (value)
    *value = *value_(index);
  if (index != buf_entries-1) {
    // move key
    memmove(key_(index), key_(index+1), (buf_entries-index-1)*suffix_bytes);
    // move value
    memmove(value_(buf_entries-2), value_(buf_entries-1), (buf_entries-index-1)*8);
  }
  buf_entries--;

  flush(&meta);
  flush(value_(index));
  fence();
  return true;
}

bool BLevel::Entry::WriteToCLevel_() {
  // TODO: let anothor thread do this? e.g. a little thread pool
  assert(0);
  for (int i = 0; i < buf_entries; ++i) {
    // clevel_()->Put(KeyAt_(i), *value_(i));
  }
}

void BLevel::Entry::ClearBuf_() {
  buf_entries = 0;
  flush(&meta);
  fence();
}


/****************************** BLevel ******************************/
BLevel::BLevel(size_t entries)
  : nr_entries_(entries), size_(0)
{
  entries_ = (Entry*)Config::Allocate(sizeof(Entry)*nr_entries_);
  entries_offset_ = (uint64_t)entries_ - Config::GetBaseAddr();
}

void BLevel::Expansion(std::vector<std::pair<uint64_t,uint64_t>>& data) {
  uint64_t old_index = 0;
  uint64_t new_index = 0;
  uint64_t last_key, last_value, cur_key;
  uint64_t prefix_len;

  size_ = 0;

#define AddEntry()                                                        \
  do {                                                                    \
    new (&entries_[new_index++]) Entry(last_key, last_value, prefix_len); \
    size_++;                                                              \
  } while (0)

  if (data.empty())
    return;

  last_key = data[0].first;
  last_value = data[0].second;

  for (int i = 1; i < data.size(); ++i) {
    cur_key = data[i].first;
    prefix_len = CommonPrefixBytes(last_key, cur_key);
    AddEntry();
    last_key = cur_key;
    last_value = data[i].second;
  }

  // last key/value
  prefix_len = CommonPrefixBytes(last_key, 0xFFFFFFFFFFFFFFFFUL);
  AddEntry();
}

// TODO: streaming store/load?
void BLevel::Expansion(BLevel* old_blevel) {
  uint64_t old_index = 0;
  uint64_t new_index = 0;
  uint64_t last_key, last_value, cur_key;
  uint64_t prefix_len;

  size_ = 0;

  // first non-empty entry
  while (old_index < old_blevel->Entries()) {
    Entry* old_entry = &old_blevel->entries_[old_index];
    if (old_entry->clevel) {
      assert(0);
      old_index++;
      break;
    } else if (old_entry->buf_entries != 0) {
      last_key = old_entry->Key(0);
      last_value = old_entry->Value(0);
      for (uint64_t i = 1; i < old_entry->buf_entries; ++i) {
        cur_key = old_entry->Key(i);
        prefix_len = CommonPrefixBytes(last_key, cur_key);
        AddEntry();
        last_key = cur_key;
        last_value = old_entry->Value(i);
      }
      old_index++;
      break;
    }
    old_index++;
  }

  // empty
  if (old_index == old_blevel->Entries())
    return;

  while (old_index < old_blevel->Entries()) {
    Entry* old_entry = &old_blevel->entries_[old_index];
    if (old_entry->clevel) {
      assert(0);
    } else if (old_entry->buf_entries != 0) {
      for (uint64_t i = 0; i < old_entry->buf_entries; ++i) {
        cur_key = old_entry->Key(i);
        prefix_len = CommonPrefixBytes(last_key, cur_key);
        AddEntry();
        last_key = cur_key;
        last_value = old_entry->Value(i);
      }
    }
    old_index++;
  }

  // last key/value
  prefix_len = CommonPrefixBytes(last_key, 0xFFFFFFFFFFFFFFFFUL);
  AddEntry();

#undef AddEntry()
}

bool BLevel::Put(uint64_t key, uint64_t value, uint64_t begin, uint64_t end) {
  assert(0);
}

bool BLevel::Get(uint64_t key, uint64_t& value, uint64_t begin, uint64_t end) const {
  assert(0);
}

bool BLevel::Delete(uint64_t key, uint64_t* value, uint64_t begin, uint64_t end) {
  assert(0);
}

}