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
BLevel::Entry::Entry(uint64_t key, uint64_t value, int prefix_len)
  : entry_key(key)
{
  buf.prefix_bytes = prefix_len;
  buf.suffix_bytes = 8 - prefix_len;
  buf.entries = 0;
  buf.max_entries = buf.MaxEntries();
  buf.Put(0, key, value);
}

BLevel::Entry::Entry(uint64_t key, int prefix_len)
  : entry_key(key)
{
  buf.prefix_bytes = prefix_len;
  buf.suffix_bytes = 8 - prefix_len;
  buf.entries = 0;
  buf.max_entries = buf.MaxEntries();
}

// #ifndef BENTRY_SORT
// void BLevel::Entry::SortedIndex_(int* sorted_index) {
//   for (int i = 0; i < buf_entries; ++i) {
//     uint64_t cur_key = KeyAt_(i);
//     int idx = 0;
//     for (idx = 0; idx < i; ++idx)
//       if (KeyAt_(sorted_index[idx]) > cur_key)
//         break;
//     memmove(&sorted_index[idx+1], &sorted_index[idx], sizeof(int)*(i-idx));
//     sorted_index[idx] = i;
//   }

//   for (int i = 0; i < buf_entries-1; ++i) {
//     if (KeyAt_(sorted_index[i]) >= KeyAt_(sorted_index[i+1])) {
//       for (int j = 0; j < buf_entries; ++j) {
//         std::cout << sorted_index[j] << " " << KeyAt_(sorted_index[j]) << std::endl;
//       }
//       std::cout << buf_entries << std::endl;
//       std::cout << entry_key << std::endl;
//       assert(KeyAt_(sorted_index[i]) < KeyAt_(sorted_index[i+1]));
//     }
//   }
// }
// #endif

bool BLevel::Entry::Put(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
  bool exist;
  int pos = buf.Find(key, exist);
  // already in, update
  if (exist) {
    *(uint64_t*)buf.pvalue(pos) = value;
    flush(buf.pvalue(pos));
    fence();
    return true;
  } else {
    if (buf.Full())
      WriteToCLevel_(mem);
    return buf.Put(pos, key, value);
  }
};

bool BLevel::Entry::Get(CLevel::MemControl* mem, uint64_t key, uint64_t& value) const {
  bool exist;
  int pos = buf.Find(key, exist);
  if (exist) {
    value = buf.value(pos);
    return true;
  } else {
    return clevel.HasSetup() ? clevel.Get(mem, key, value) : false;
  }
}

bool BLevel::Entry::Delete(CLevel::MemControl* mem, uint64_t key, uint64_t* value) {
  bool exist;
  int pos = buf.Find(key, exist);
  if (exist) {
    if (value)
      *value = buf.value(pos);
    return buf.Delete(pos);
  } else {
    return clevel.HasSetup() ? clevel.Delete(mem, key, value) : false;
  }
}

void BLevel::Entry::WriteToCLevel_(CLevel::MemControl* mem) {
  // TODO: let anothor thread do this? e.g. a little thread pool
  auto start = std::chrono::high_resolution_clock::now();

  if (!clevel.HasSetup()) {
    // data will be moved from blevel.buf to clevel.root.buf
    // blevel.buf will be cleared inside Setup()
    clevel.Setup(mem, buf);
  } else {
    for (int i = 0; i < buf.entries; ++i)
      assert(clevel.Put(mem, buf.key(i, entry_key), buf.value(i)) == true);
    buf.Clear();
  }

  auto stop = std::chrono::high_resolution_clock::now();
  clevel_time += std::chrono::duration_cast<std::chrono::microseconds>(stop-start).count();
}


/****************************** BLevel ******************************/
BLevel::BLevel(size_t data_size)
  : nr_entries_(0), size_(0)
{
  entries_ = (Entry*)Config::Allocate(sizeof(Entry)*((data_size+1+BLEVEL_EXPAND_BUF_KEY-1)/BLEVEL_EXPAND_BUF_KEY));
  entries_offset_ = (uint64_t)entries_ - Config::GetBaseAddr();
}

void BLevel::ExpandSetup_(ExpandData& data) {
  data.buf_count = 0;
  data.new_addr = entries_;
  data.zero_entry = true;
}

void BLevel::ExpandPut_(ExpandData& data, uint64_t key, uint64_t value) {
  if (data.buf_count == BLEVEL_EXPAND_BUF_KEY) {
    if (data.zero_entry && data.key_buf[0] != 0) {
      int prefix_len = CommonPrefixBytes(0UL, key);
#ifdef STREAMING_STORE
      Entry new_entry;
      new (&new_entry) Entry(0UL, prefix_len);
      for (int i = 0; i < data.buf_count; ++i)
        new_entry.Put(data.key_buf[i], data.value_buf[i]);
      stream_store_entry(data.new_addr, &new_entry);
#else
      Entry* new_entry = new (data.new_addr) Entry(0UL, prefix_len);
      for (int i = 0; i < data.buf_count; ++i)
        new_entry->Put(data.key_buf[i], data.value_buf[i]);
#endif
    } else {
      int prefix_len = CommonPrefixBytes(data.key_buf[0], key);
#ifdef STREAMING_STORE
      Entry new_entry;
      new (&new_entry) Entry(data.key_buf[0], data.value_buf[0], prefix_len);
      for (int i = 1; i < data.buf_count; ++i)
        new_entry.Put(data.key_buf[i], data.value_buf[i]);
      stream_store_entry(data.new_addr, &new_entry);
#else
      Entry* new_entry = new (data.new_addr) Entry(data.key_buf[0], data.value_buf[0], prefix_len);
      for (int i = 1; i < data.buf_count; ++i)
        new_entry->Put(data.key_buf[i], data.value_buf[i]);
#endif
    }
    data.new_addr++;
    data.buf_count = 0;
    data.zero_entry = false;
    nr_entries_++;
  }
  data.key_buf[data.buf_count] = key;
  data.value_buf[data.buf_count] = value;
  data.buf_count++;
  size_++;
}

void BLevel::ExpandFinish_(ExpandData& data) {
  assert(data.zero_entry != true);
  if (data.buf_count != 0) {
    int prefix_len = CommonPrefixBytes(data.key_buf[0], 0xFFFFFFFFFFFFFFFFUL);
#ifdef STREAMING_STORE
    Entry new_entry;
    new (&new_entry) Entry(data.key_buf[0], data.value_buf[0], prefix_len);
    data.new_addr++;
    for (int i = 1; i < data.buf_count; ++i)
      new_entry.Put(data.key_buf[i], data.value_buf[i]);
    stream_store_entry(data.new_addr, &new_entry);
#else
    Entry* new_entry = new (data.new_addr) Entry(data.key_buf[0], data.value_buf[0], prefix_len);
    data.new_addr++;
    for (int i = 1; i < data.buf_count; ++i)
      new_entry->Put(data.key_buf[i], data.value_buf[i]);
#endif
    nr_entries_++;
  }
}

void BLevel::Expansion(std::vector<std::pair<uint64_t,uint64_t>>& data) {
  uint64_t old_index = 0;
  uint64_t new_index = 0;
  uint64_t cur_key, cur_value;
  ExpandData expand_meta;

  size_ = 0;

  if (data.empty())
    return;

  ExpandSetup_(expand_meta);

  for (int i = 0; i < data.size(); ++i) {
    cur_key = data[i].first;
    cur_value = data[i].second;
    ExpandPut_(expand_meta, cur_key, cur_value);
  }

  ExpandFinish_(expand_meta);
}

void BLevel::Expansion(BLevel* old_blevel) {
  uint64_t old_index = 0;
  uint64_t new_index = 0;
  uint64_t cur_key, cur_value;
  ExpandData expand_meta;
  Entry* old_entry;

  size_ = 0;

  std::vector<std::pair<uint64_t,uint64_t>> clevel_scan;

  ExpandSetup_(expand_meta);

#ifdef STREAMING_STORE
  Entry in_mem_entry;
  Entry new_entry;
  old_entry = &in_mem_entry;
#endif

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
              && old_entry->key_(buf_idx) < clevel_scan[vec_idx].first)) {
            cur_key = old_entry->Key(buf_idx);
            cur_value = old_entry->Value(buf_idx);
#else
              && old_entry->Key(sorted_index[buf_idx]) < clevel_scan[vec_idx].first)) {
            cur_key = old_entry->Key(sorted_index[buf_idx]);
            cur_value = old_entry->Value(sorted_index[buf_idx]);
#endif
            ExpandPut_(expand_meta, cur_key, cur_value);
            buf_idx++;
          } else {
            cur_key = clevel_scan[vec_idx].first;
            cur_value = clevel_scan[vec_idx].second;
            ExpandPut_(expand_meta, cur_key, cur_value);
            vec_idx++;
          }
        }
      }
    } else if (old_entry->buf_entries != 0) {
      for (uint64_t i = 0; i < old_entry->buf_entries; ++i) {
#ifdef BENTRY_SORT
        cur_key = old_entry->Key(i);
        cur_value = old_entry->Value(i);
#else
        cur_key = old_entry->Key(sorted_index[i]);
        cur_value = old_entry->Value(sorted_index[i]);
#endif
        ExpandPut_(expand_meta, cur_key, cur_value);
      }
    }
    old_index++;
  }

  ExpandFinish_(expand_meta);
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