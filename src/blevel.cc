#include <cstring>
#include <cassert>
#include <vector>
#include <iostream>
#include <chrono>
#include "blevel.h"
#include "config.h"

namespace combotree {

namespace { // anonymous namespace

int64_t clevel_time = 0;

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
  for (int i = 0; i < 8; ++i)
    if (((char*)&a)[7-i] != ((char*)&b)[7-i])
      return i;
  return 8;
}

#ifdef STREAMING_STORE
void stream_load_entry(void* dest, void* source) {
  uint8_t* dst = (uint8_t*)dest;
  uint8_t* src = (uint8_t*)source;
#if __SSE2__
  for (int i = 0; i < 8; ++i)
    *(__m128i*)(dst+16*i) = _mm_stream_load_si128((__m128i*)(src+16*i));
#elif __AVX2__
  for (int i = 0; i < 4; ++i)
    *(__m256i*)(dst+32*i) = _mm256_stream_load_si256((__m256i*)(src+32*i));
#elif __AVX512VL__
  *(__m512i*)dst = _mm512_stream_load_si512((__m512i*)src);
  *(__m512i*)(dst+64) = _mm512_stream_load_si512((__m512i*)(src+64));
#else
  static_assert(0, "stream_load_entry");
#endif
}

void stream_store_entry(void* dest, void* source) {
  uint8_t* dst = (uint8_t*)dest;
  uint8_t* src = (uint8_t*)source;
#if __SSE2__
  for (int i = 0; i < 8; ++i)
    _mm_stream_si128((__m128i*)(dst+16*i), *(__m128i*)(src+16*i));
#elif __AVX2__
  for (int i = 0; i < 4; ++i)
    _mm256_stream_si256((__m256i*)(dst+32*i), *(__m256i*)(src+32*i));
#elif __AVX512VL__
  _mm512_stream_si512((__m512i*)dst, *(__m512i*)src);
  _mm512_stream_si512((__m512i*)(dst+64), *(__m512i*)(src+64));
#else
  static_assert(0, "stream_load_entry");
#endif
}
#endif // STREAMING_STORE

} // anonymous namespace


/************************** BLevel::Entry ***************************/
BLevel::Entry::Entry() : entry_key(0), meta(0) {}

BLevel::Entry::Entry(uint64_t key, uint64_t value, int prefix_len)
  : entry_key(key), prefix_bytes(prefix_len),
    suffix_bytes(8-prefix_len), buf_entries(0)
{
  CalcMaxEntries_();
  Put(key, value);
}

BLevel::Entry::Entry(uint64_t key, int prefix_len)
  : entry_key(key), prefix_bytes(prefix_len),
    suffix_bytes(8-prefix_len), buf_entries(0)
{
  CalcMaxEntries_();
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
  return (CLevel*)(clevel + Config::CLevelSlab()->BaseAddr());
}

void BLevel::Entry::SetKey_(int index, uint64_t key) {
  // little endian
  memcpy(key_(index), &key, suffix_bytes);
}

#ifndef BENTRY_SORT
void BLevel::Entry::SortedIndex_(int* sorted_index) {
  for (int i = 0; i < buf_entries; ++i) {
    uint64_t cur_key = KeyAt_(i);
    int idx = 0;
    for (idx = 0; idx < i; ++idx)
      if (KeyAt_(sorted_index[idx]) > cur_key)
        break;
    memmove(&sorted_index[idx+1], &sorted_index[idx], sizeof(int)*(i-idx));
    sorted_index[idx] = i;
  }

  for (int i = 0; i < buf_entries-1; ++i) {
    if (KeyAt_(sorted_index[i]) >= KeyAt_(sorted_index[i+1])) {
      for (int j = 0; j < buf_entries; ++j) {
        std::cout << sorted_index[j] << " " << KeyAt_(sorted_index[j]) << std::endl;
      }
      std::cout << buf_entries << std::endl;
      std::cout << entry_key << std::endl;
      assert(KeyAt_(sorted_index[i]) < KeyAt_(sorted_index[i+1]));
    }
  }
}
#endif

int BLevel::Entry::Find_(uint64_t key, bool& find) const {
#ifdef BENTRY_SORT
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
#else
  for (int i = 0; i < buf_entries; ++i) {
    if (KeyAt_(i) == key) {
      find = true;
      return i;
    }
  }
  find = false;
  return 0;
#endif
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
  int index = Find_(key, exist);
  // already in, update
  if (exist) {
    *value_(index) = value;
    flush(value_(index));
    fence();
    return true;
  }
  // not exist
#ifdef BENTRY_SORT
  if (index != buf_entries) {
    // move key
    memmove(key_(index+1), key_(index), (buf_entries-index)*suffix_bytes);
    // move value
    memmove(value_(buf_entries), value_(buf_entries-1), (buf_entries-index)*8);
  }
  SetKey_(index, key);
  *value_(index) = value;
#else
  SetKey_(buf_entries, key);
  *value_(buf_entries) = value;
#endif
  buf_entries++;

  flush(&meta);
  flush(value_(index));
  fence();
  return true;
};

bool BLevel::Entry::Get(uint64_t key, uint64_t& value) const {
  bool exist;
  int index = Find_(key, exist);
  if (exist) {
    value = Value(index);
    return true;
  }
  if (clevel) {
    return clevel_()->Get(Config::CLevelMem(), key, value) == Status::OK;
  } else {
    return false;
  }
}

bool BLevel::Entry::Delete(uint64_t key, uint64_t* value) {
  bool exist;
  int index = Find_(key, exist);
  if (!exist) {
    if (clevel)
      return clevel_()->Delete(Config::CLevelMem(), key) == Status::OK;
    return false;
  }
  if (value)
    *value = *value_(index);
  if (index != buf_entries - 1) {
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
  if (clevel == 0) {
    CLevel* tmp = Config::CLevelSlab()->Allocate();
    tmp->InitLeaf(Config::CLevelMem());
    clevel = (uint64_t)tmp - Config::CLevelSlab()->BaseAddr();
    // flush and fence will be done by ClearBuf_()
  }

  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < buf_entries; ++i)
    clevel_()->Put(Config::CLevelMem(), Key(i), Value(i));

  auto stop = std::chrono::high_resolution_clock::now();
  clevel_time += std::chrono::duration_cast<std::chrono::microseconds>(stop-start).count();
  return true;
}

void BLevel::Entry::ClearBuf_() {
  buf_entries = 0;
  flush(&meta);
  fence();
}


/****************************** BLevel ******************************/
BLevel::BLevel(size_t entries)
  : nr_entries_(entries+1), size_(0)
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

#ifdef STREAMING_STORE
  Entry new_entry;
#define AddEntry()                                                        \
  do {                                                                    \
    assert(prefix_len != 8);                                              \
    new (&new_entry) Entry(last_key, last_value, prefix_len);             \
    stream_store_entry(&entries_[new_index++], &new_entry);               \
    size_++;                                                              \
  } while (0)
#else
#define AddEntry()                                                        \
  do {                                                                    \
    assert(prefix_len != 8);                                              \
    new (&entries_[new_index++]) Entry(last_key, last_value, prefix_len); \
    size_++;                                                              \
  } while (0)
#endif

  if (data.empty())
    return;

  last_key = data[0].first;
  last_value = data[0].second;

  if (last_key != 0) {
    // add zero entry
    prefix_len = CommonPrefixBytes(0UL, last_key);
    new (&entries_[new_index++]) Entry(0, prefix_len);
  } else {
    nr_entries_--;
  }

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

  std::vector<std::pair<uint64_t,uint64_t>> clevel_scan;

  Entry* old_entry;
#ifdef STREAMING_STORE
  Entry in_mem_entry;
  Entry new_entry;
  old_entry = &in_mem_entry;
#endif

  // first non-empty entry
  while (old_index < old_blevel->Entries()) {
#ifdef STREAMING_STORE
    stream_load_entry(&in_mem_entry, &old_blevel->entries_[old_index]);
#else
    old_entry = &old_blevel->entries_[old_index];
#endif

#ifndef BENTRY_SORT
    int sorted_index[sizeof(old_entry->buf)/(8+1)];
    old_entry->SortedIndex_(sorted_index);
#endif
    if (old_entry->clevel) {
      clevel_scan.clear();
      size_t scan_size = 0;
      old_entry->clevel_()->Scan(Config::CLevelMem(), UINT64_MAX, UINT64_MAX, scan_size,
        [](uint64_t key, uint64_t value, void* arg) {
          std::vector<std::pair<uint64_t,uint64_t>>* result = (std::vector<std::pair<uint64_t,uint64_t>>*)arg;
          result->emplace_back(key, value);
        }, &clevel_scan);

      assert(scan_size == clevel_scan.size());

      if (clevel_scan.size() != 0 || old_entry->buf_entries != 0) {
        int vec_idx = 0;
        int buf_idx = 0;
        if (clevel_scan.size() == 0 || (old_entry->buf_entries != 0 &&
#ifdef BENTRY_SORT
            old_entry->Key(0) < clevel_scan[0].first)) {
          last_key = old_entry->Key(0);
          last_value = old_entry->Value(0);
#else
            old_entry->Key(sorted_index[0]) < clevel_scan[0].first)) {
          last_key = old_entry->Key(sorted_index[0]);
          last_value = old_entry->Value(sorted_index[0]);
#endif
          buf_idx++;
        } else {
          last_key = clevel_scan[0].first;
          last_value = clevel_scan[0].second;
          vec_idx++;
        }
        if (last_key != 0) {
          // add zero entry
          prefix_len = CommonPrefixBytes(0UL, last_key);
          new (&entries_[new_index++]) Entry(0, prefix_len);
        } else {
          nr_entries_--;
        }
        while (vec_idx != clevel_scan.size() || buf_idx != old_entry->buf_entries) {
          if (vec_idx == clevel_scan.size() || (buf_idx != old_entry->buf_entries
#ifdef BENTRY_SORT
              && old_entry->Key(buf_idx) < clevel_scan[vec_idx].first)) {
            cur_key = old_entry->Key(buf_idx);
#else
              && old_entry->Key(sorted_index[buf_idx]) < clevel_scan[vec_idx].first)) {
            cur_key = old_entry->Key(sorted_index[buf_idx]);
#endif
            prefix_len = CommonPrefixBytes(last_key, cur_key);
            AddEntry();
            last_key = cur_key;
#ifdef BENTRY_SORT
            last_value = old_entry->Value(buf_idx);
#else
            last_value = old_entry->Value(sorted_index[buf_idx]);
#endif
            buf_idx++;
          } else {
            cur_key = clevel_scan[vec_idx].first;
            prefix_len = CommonPrefixBytes(last_key, cur_key);
            AddEntry();
            last_key = cur_key;
            last_value = clevel_scan[vec_idx].second;
            vec_idx++;
          }
        }
        old_index++;
        break;
      }
    } else if (old_entry->buf_entries != 0) {
#ifdef BENTRY_SORT
      last_key = old_entry->Key(0);
      last_value = old_entry->Value(0);
#else
      last_key = old_entry->Key(sorted_index[0]);
      last_value = old_entry->Value(sorted_index[0]);
#endif
      if (last_key != 0) {
        // add zero entry
        prefix_len = CommonPrefixBytes(0UL, last_key);
        new (&entries_[new_index++]) Entry(0, prefix_len);
      } else {
        nr_entries_--;
      }
      for (uint64_t i = 1; i < old_entry->buf_entries; ++i) {
#ifdef BENTRY_SORT
        cur_key = old_entry->Key(i);
#else
        cur_key = old_entry->Key(sorted_index[i]);
#endif
        prefix_len = CommonPrefixBytes(last_key, cur_key);
        AddEntry();
        last_key = cur_key;
#ifdef BENTRY_SORT
        last_value = old_entry->Value(i);
#else
        last_value = old_entry->Value(sorted_index[i]);
#endif
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
#ifdef STREAMING_STORE
    stream_load_entry(&in_mem_entry, &old_blevel->entries_[old_index]);
#else
    old_entry = &old_blevel->entries_[old_index];
#endif

#ifndef BENTRY_SORT
    int sorted_index[sizeof(old_entry->buf)/(8+1)];
    old_entry->SortedIndex_(sorted_index);
#endif
    if (old_entry->clevel) {
      clevel_scan.clear();
      size_t scan_size = 0;
      old_entry->clevel_()->Scan(Config::CLevelMem(), UINT64_MAX, UINT64_MAX, scan_size,
        [](uint64_t key, uint64_t value, void* arg) {
          std::vector<std::pair<uint64_t,uint64_t>>* result = (std::vector<std::pair<uint64_t,uint64_t>>*)arg;
          result->emplace_back(key, value);
        }, &clevel_scan);

      assert(scan_size == clevel_scan.size());

      if (clevel_scan.size() != 0 || old_entry->buf_entries != 0) {
        int vec_idx = 0;
        int buf_idx = 0;
        while (vec_idx != clevel_scan.size() || buf_idx != old_entry->buf_entries) {
          if (vec_idx == clevel_scan.size() || (buf_idx != old_entry->buf_entries
#ifdef BENTRY_SORT
              && old_entry->Key(buf_idx) < clevel_scan[vec_idx].first)) {
            cur_key = old_entry->Key(buf_idx);
#else
              && old_entry->Key(sorted_index[buf_idx]) < clevel_scan[vec_idx].first)) {
            cur_key = old_entry->Key(sorted_index[buf_idx]);
#endif
            prefix_len = CommonPrefixBytes(last_key, cur_key);
            AddEntry();
            last_key = cur_key;
#ifdef BENTRY_SORT
            last_value = old_entry->Value(buf_idx);
#else
            last_value = old_entry->Value(sorted_index[buf_idx]);
#endif
            buf_idx++;
          } else {
            cur_key = clevel_scan[vec_idx].first;
            prefix_len = CommonPrefixBytes(last_key, cur_key);
            AddEntry();
            last_key = cur_key;
            last_value = clevel_scan[vec_idx].second;
            vec_idx++;
          }
        }
      }
    } else if (old_entry->buf_entries != 0) {
      for (uint64_t i = 0; i < old_entry->buf_entries; ++i) {
#ifdef BENTRY_SORT
        cur_key = old_entry->Key(i);
#else
        cur_key = old_entry->Key(sorted_index[i]);
#endif
        prefix_len = CommonPrefixBytes(last_key, cur_key);
        AddEntry();
        last_key = cur_key;
#ifdef BENTRY_SORT
        last_value = old_entry->Value(i);
#else
        last_value = old_entry->Value(sorted_index[i]);
#endif
      }
    }
    old_index++;
  }

  // last key/value
  prefix_len = CommonPrefixBytes(last_key, 0xFFFFFFFFFFFFFFFFUL);
  AddEntry();

#undef AddEntry
}

uint64_t BLevel::Find_(uint64_t key, uint64_t begin, uint64_t end) const {
  assert(begin < Entries());
  assert(end < Entries());
  int_fast32_t left = begin;
  int_fast32_t right = end;
  // binary search
  while (left <= right) {
    int middle = (left + right) / 2;
    uint64_t mid_key = entries_[middle].entry_key;
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

bool BLevel::Put(uint64_t key, uint64_t value, uint64_t begin, uint64_t end) {
  uint64_t idx = Find_(key, begin, end);
  if (entries_[idx].Put(key, value)) {
    size_++;
    return true;
  }
  return false;
}

bool BLevel::Get(uint64_t key, uint64_t& value, uint64_t begin, uint64_t end) const {
  uint64_t idx = Find_(key, begin, end);
  return entries_[idx].Get(key, value);
}

bool BLevel::Delete(uint64_t key, uint64_t* value, uint64_t begin, uint64_t end) {
  uint64_t idx = Find_(key, begin, end);
  if (entries_[idx].Delete(key, value)) {
    size_--;
    return true;
  }
  return false;
}

size_t BLevel::CountCLevel() const {
  size_t cnt = 0;
  for (uint64_t i = 0; i < Entries(); ++i)
    if (entries_[i].clevel)
      cnt++;
  return cnt;
}

void BLevel::PrefixCompression() const {
  uint64_t cnt[9];
  for (int i = 0; i < 9; ++i)
    cnt[i] = 0;
  for (uint64_t i = 0; i < Entries(); ++i)
    cnt[entries_[i].suffix_bytes]++;
  for (int i = 1; i < 9; ++i)
    std::cout << "sufix " << i << " count: " << cnt[i] << std::endl;
}

int64_t BLevel::CLevelTime() const {
  return clevel_time;
}

}