#pragma once

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <vector>
#include "kvbuffer.h"
#include "clevel.h"
#include "pmem.h"
#include "combotree_config.h"

namespace combotree {

class Test;

class BLevel {
 public:
  BLevel(size_t entries);
  ~BLevel();

  bool Put(uint64_t key, uint64_t value, uint64_t begin, uint64_t end);
  bool Get(uint64_t key, uint64_t& value, uint64_t begin, uint64_t end) const;
  bool Delete(uint64_t key, uint64_t* value, uint64_t begin, uint64_t end);

  void Expansion(BLevel* old_blevel);
  void Expansion(std::vector<std::pair<uint64_t,uint64_t>>& data);

  // statistic
  size_t CountCLevel() const;
  void PrefixCompression() const;
  int64_t CLevelTime() const;
  uint64_t Usage() const;

  ALWAYS_INLINE size_t Size() const { return size_; }
  ALWAYS_INLINE size_t Entries() const { return nr_entries_; }
  ALWAYS_INLINE uint64_t EntryKey(int index) const { return entries_[index].entry_key; }
  ALWAYS_INLINE uint64_t MinEntryKey() const { return entries_[1].entry_key; }
  ALWAYS_INLINE uint64_t MaxEntryKey() const { return entries_[Entries()-1].entry_key; }

  friend Test;

 private:
  struct __attribute__((aligned(64))) Entry {
    uint64_t entry_key;
    CLevel clevel;
    KVBuffer<48+64,8> buf;  // contains 2 bytes meta

    Entry();
    Entry(uint64_t key, int prefix_len);
    Entry(uint64_t key, uint64_t value, int prefix_len);

    ALWAYS_INLINE uint64_t key(int idx) const {
      return buf.key(idx, entry_key);
    }

    ALWAYS_INLINE uint64_t value(int idx) const {
      return buf.value(idx);
    }

    bool Put(CLevel::MemControl* mem, uint64_t key, uint64_t value);
    bool Get(CLevel::MemControl* mem, uint64_t key, uint64_t& value) const;
    bool Delete(CLevel::MemControl* mem, uint64_t key, uint64_t* value);

    void FlushToCLevel(CLevel::MemControl* mem);
  }; // Entry

  static_assert(sizeof(BLevel::Entry) == 128, "sizeof(BLevel::Entry) != 128");

  struct ExpandData {
    Entry* new_addr;
    uint64_t key_buf[BLEVEL_EXPAND_BUF_KEY];
    uint64_t value_buf[BLEVEL_EXPAND_BUF_KEY];
    int buf_count;
    bool zero_entry;

    ExpandData(Entry* entries) {
      buf_count = 0;
      new_addr = entries;
      zero_entry = true;
    }

    void FlushToEntry(Entry* entry, int prefix_len) {
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

      buf_count = 0;
    }
  };

  // member
  void* pmem_addr_;
  size_t mapped_len_;
  std::string pmem_file_;
  static int file_id_;

  uint64_t entries_offset_;                     // pmem file offset
  Entry* __attribute__((aligned(64))) entries_; // current mmaped address
  size_t nr_entries_;
  std::atomic<size_t> size_;
  CLevel::MemControl clevel_mem_;

  // function
  uint64_t Find_(uint64_t key, uint64_t begin, uint64_t end) const;
  void ExpandSetup_(ExpandData& data);
  void ExpandPut_(ExpandData& data, uint64_t key, uint64_t value);
  void ExpandFinish_(ExpandData& data);
};

}