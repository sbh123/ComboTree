#include <cstring>
#include <cassert>
#include <iostream>
#include <chrono>
#include <shared_mutex>
#include "combotree_config.h"
#include "blevel.h"

namespace combotree {

int BLevel::file_id_ = 0;

namespace { // anonymous namespace

std::atomic<int64_t> clevel_time = 0;

ALWAYS_INLINE int CommonPrefixBytes(uint64_t a, uint64_t b) {
  // the result of clz is undefined if arg is 0
  int leading_zero_cnt = (a ^ b) == 0 ? 64 : __builtin_clzll(a ^ b);
  return leading_zero_cnt / 8;
}

#ifdef STREAMING_LOAD
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
#endif // STREAMING_LOAD

#ifdef STREAMING_STORE
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
  static_assert(0, "stream_store_entry");
#endif
}
#endif // STREAMING_STORE

} // anonymous namespace

void BLevel::ExpandData::FlushToEntry(Entry* entry, int prefix_len, CLevel::MemControl* mem) {
  while (buf_count > entry->buf.max_entries) {
    // flush last entry.max_entries data to clevel
    // copy value
    memcpy(entry->buf.pvalue(entry->buf.max_entries-1),
           &value_buf[BLEVEL_EXPAND_BUF_KEY-buf_count], 8*entry->buf.max_entries);
    // copy key
    for (int i = 0; i < entry->buf.max_entries; ++i)
      memcpy(entry->buf.pkey(i), &key_buf[i+buf_count-entry->buf.max_entries], 8 - prefix_len);
    entry->buf.entries = entry->buf.max_entries;
    entry->FlushToCLevel(mem);
    buf_count -= entry->buf.max_entries;
  }
#ifdef STREAMING_STORE
  Entry in_mem(entry->entry_key, prefix_len);
  // copy value
  memcpy(in_mem.buf.pvalue(buf_count-1),
         &value_buf[BLEVEL_EXPAND_BUF_KEY-buf_count], 8*buf_count);
  // copy key
  for (int i = 0; i < buf_count; ++i)
    memcpy(in_mem.buf.pkey(i), &key_buf[i], 8 - prefix_len);
  in_mem.buf.entries = buf_count;
  stream_store_entry(entry, &in_mem);
#else
  // copy value
  memcpy(entry->buf.pvalue(buf_count-1),
         &value_buf[BLEVEL_EXPAND_BUF_KEY-buf_count], 8*buf_count);
  // copy key
  for (int i = 0; i < buf_count; ++i)
    memcpy(entry->buf.pkey(i), &key_buf[i], 8 - prefix_len);
  entry->buf.entries = buf_count;
  flush(entry);
  flush((uint8_t*)entry+64);
  fence();
#endif // STREAMING_STORE

  buf_count = 0;
}


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

// return true if not exist before, return false if update.
bool BLevel::Entry::Put(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
  bool exist;
  int pos = buf.Find(key, exist);
  // already in, update
  if (exist) {
    *(uint64_t*)buf.pvalue(pos) = value;
    flush(buf.pvalue(pos));
    fence();
    return false;
  } else {
#ifdef BUF_SORT
    if (buf.Full()) {
#else
    if ((!clevel.HasSetup() && buf.entries == buf.max_entries - 1) || buf.Full()) {
#endif
      FlushToCLevel(mem);
      pos = 0;
    }
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

void BLevel::Entry::FlushToCLevel(CLevel::MemControl* mem) {
  // TODO: let anothor thread do this? e.g. a little thread pool
  Timer timer;
  timer.Start();

  if (!clevel.HasSetup()) {
    clevel.Setup(mem, buf);
  } else {
    for (int i = 0; i < buf.entries; ++i)
      assert(clevel.Put(mem, buf.key(i, entry_key), buf.value(i)) == true);
  }
  buf.Clear();

  clevel_time.fetch_add(timer.End());
}


/****************************** BLevel ******************************/
BLevel::BLevel(size_t data_size)
  : nr_entries_(0), size_(0),
    clevel_mem_(CLEVEL_PMEM_FILE, CLEVEL_PMEM_FILE_SIZE)
#ifndef NO_LOCK
    , lock_(nullptr)
#endif
{
  pmem_file_ = std::string(BLEVEL_PMEM_FILE) + std::to_string(file_id_);
  int is_pmem;
  std::filesystem::remove(pmem_file_);
  size_t file_size = sizeof(Entry)*((data_size+1+BLEVEL_EXPAND_BUF_KEY-1)/BLEVEL_EXPAND_BUF_KEY);
  pmem_addr_ = pmem_map_file(pmem_file_.c_str(), file_size + 64,
               PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666, &mapped_len_, &is_pmem);
  assert(is_pmem == 1);
  if (pmem_addr_ == nullptr) {
    perror("BLevel::BLevel(): pmem_map_file");
    exit(1);
  }

  // aligned at 64-bytes
  entries_ = (Entry*)pmem_addr_;
  if (((uintptr_t)entries_ & (uintptr_t)63) != 0) {
    // not aligned
    entries_ = (Entry*)(((uintptr_t)entries_+64) & ~(uintptr_t)63);
  }

  entries_offset_ = (uint64_t)entries_ - (uint64_t)pmem_addr_;
}

BLevel::~BLevel() {
  if (pmem_addr_ != nullptr) {
    pmem_unmap(pmem_addr_, mapped_len_);
    std::filesystem::remove(pmem_file_);
  }
#ifndef NO_LOCK
  if (lock_) delete lock_;
#endif
}

void BLevel::ExpandPut_(ExpandData& data, uint64_t key, uint64_t value) {
  if (data.buf_count == BLEVEL_EXPAND_BUF_KEY) {
    // buf full, add a new entry
    if (data.zero_entry && data.key_buf[0] != 0) {
      int prefix_len = CommonPrefixBytes(0UL, key);
      Entry* new_entry = new (data.new_addr) Entry(0UL, prefix_len);
      data.FlushToEntry(new_entry, prefix_len, &clevel_mem_);
    } else {
      int prefix_len = CommonPrefixBytes(data.key_buf[0], key);
      Entry* new_entry = new (data.new_addr) Entry(data.key_buf[0], prefix_len);
      data.FlushToEntry(new_entry, prefix_len, &clevel_mem_);
    }
    data.new_addr++;
    data.zero_entry = false;
    nr_entries_++;
  }
  data.key_buf[data.buf_count] = key;
  data.value_buf[BLEVEL_EXPAND_BUF_KEY - data.buf_count - 1] = value;
  data.buf_count++;
  size_++;
}

void BLevel::ExpandFinish_(ExpandData& data) {
  assert(data.zero_entry != true);
  if (data.buf_count != 0) {
    int prefix_len = CommonPrefixBytes(data.key_buf[0], 0xFFFFFFFFFFFFFFFFUL);
    Entry* new_entry = new (data.new_addr) Entry(data.key_buf[0], prefix_len);
    data.FlushToEntry(new_entry, prefix_len, &clevel_mem_);
    data.new_addr++;
    nr_entries_++;
  }
}

void BLevel::Expansion(std::vector<std::pair<uint64_t,uint64_t>>& data) {
  size_ = 0;

  if (data.empty())
    return;

  ExpandData expand_meta(entries_);
  for (size_t i = 0; i < data.size(); ++i)
    ExpandPut_(expand_meta, data[i].first, data[i].second);
  ExpandFinish_(expand_meta);

#ifndef NO_LOCK
  // plus one because of scan
  lock_ = new std::shared_mutex[Entries()+1];
#endif
}

void BLevel::Expansion(BLevel* old_blevel) {
  uint64_t old_index = 0;
  ExpandData expand_meta(entries_);
  Entry* old_entry;
  CLevel::MemControl* old_mem = &old_blevel->clevel_mem_;

  size_ = 0;

#ifdef STREAMING_LOAD
  Entry in_mem_entry(0,0);
  old_entry = &in_mem_entry;
#endif

  while (old_index < old_blevel->Entries()) {
#ifndef NO_LOCK
    // lock before streaming load
    std::lock_guard<std::shared_mutex> lock(old_blevel->lock_[old_index]);
#endif
#ifdef STREAMING_LOAD
    stream_load_entry(&in_mem_entry, &old_blevel->entries_[old_index]);
#else
    old_entry = &old_blevel->entries_[old_index];
#endif

    if (old_entry->clevel.HasSetup()) {
      expand_meta.clevel_count++;
      Entry::Iter biter(old_entry, old_mem);
      uint64_t total_cnt = 0;
      do {
        total_cnt++;
        ExpandPut_(expand_meta, biter.key(), biter.value());
      } while(biter.next());
      expand_meta.clevel_data_count += total_cnt - old_entry->buf.entries;
    } else if (!old_entry->buf.Empty()) {
#ifdef BUF_SORT
      for (uint64_t i = 0; i < old_entry->buf.entries; ++i)
        ExpandPut_(expand_meta, old_entry->key(i), old_entry->value(i));
#else
      int sorted_index[16];
      old_entry->buf.GetSortedIndex(sorted_index);
      for (uint64_t i = 0; i < old_entry->buf.entries; ++i)
        ExpandPut_(expand_meta, old_entry->key(sorted_index[i]), old_entry->value(sorted_index[i]));
#endif
    }
    old_index++;
  }

  ExpandFinish_(expand_meta);

  LOG(Debug::INFO, "data in clevel: %ld, clevel count: %ld, pairs per clevel: %lf",
      expand_meta.clevel_data_count, expand_meta.clevel_count, (double)expand_meta.clevel_data_count/(double)expand_meta.clevel_count);

#ifndef NO_LOCK
  // plus one because of scan
  lock_ = new std::shared_mutex[Entries()+1];
#endif
}

uint64_t BLevel::Find_(uint64_t key, uint64_t begin, uint64_t end) const {
  assert(begin < Entries());
  assert(end < Entries());
  int left = begin;
  int right = end;
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
#ifndef NO_LOCK
  std::lock_guard<std::shared_mutex> lock(lock_[idx]);
#endif
  if (entries_[idx].Put(&clevel_mem_, key, value)) {
    size_++;
    return true;
  }
  return false;
}

bool BLevel::Get(uint64_t key, uint64_t& value, uint64_t begin, uint64_t end) const {
  uint64_t idx = Find_(key, begin, end);
#ifndef NO_LOCK
  std::shared_lock<std::shared_mutex> lock(lock_[idx]);
#endif
  return entries_[idx].Get((CLevel::MemControl*)&clevel_mem_, key, value);
}

bool BLevel::Delete(uint64_t key, uint64_t* value, uint64_t begin, uint64_t end) {
  uint64_t idx = Find_(key, begin, end);
#ifndef NO_LOCK
  std::lock_guard<std::shared_mutex> lock(lock_[idx]);
#endif
  if (entries_[idx].Delete(&clevel_mem_, key, value)) {
    size_--;
    return true;
  }
  return false;
}

size_t BLevel::CountCLevel() const {
  size_t cnt = 0;
  for (uint64_t i = 0; i < Entries(); ++i)
    if (entries_[i].clevel.HasSetup())
      cnt++;
  return cnt;
}

void BLevel::PrefixCompression() const {
  uint64_t cnt[9];
  for (int i = 0; i < 9; ++i)
    cnt[i] = 0;
  for (uint64_t i = 0; i < Entries(); ++i)
    cnt[entries_[i].buf.suffix_bytes]++;
  for (int i = 1; i < 9; ++i)
    std::cout << "suffix " << i << " count: " << cnt[i] << std::endl;
}

int64_t BLevel::CLevelTime() const {
  return clevel_time;
}

uint64_t BLevel::Usage() const {
  return clevel_mem_.Usage() + Entries() * sizeof(Entry);
}

}