#pragma once

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <vector>
#include "clevel.h"
#include "pmem.h"

namespace combotree {

class Test;

class BLevel {
 public:
  BLevel(size_t entries);

  bool Put(uint64_t key, uint64_t value, uint64_t begin, uint64_t end);
  bool Get(uint64_t key, uint64_t& value, uint64_t begin, uint64_t end) const;
  bool Delete(uint64_t key, uint64_t* value, uint64_t begin, uint64_t end);

  void Expansion(BLevel* old_blevel);
  void Expansion(std::vector<std::pair<uint64_t,uint64_t>>& data);

  size_t CountCLevel() const;
  inline __attribute__((always_inline)) size_t Size() const { return size_; }
  inline __attribute__((always_inline)) size_t Entries() const { return nr_entries_; }
  inline __attribute__((always_inline)) uint64_t EntryKey(int index) const { return entries_[index].entry_key; }
  inline __attribute__((always_inline)) uint64_t MinEntryKey() const { return entries_[1].entry_key; }
  inline __attribute__((always_inline)) uint64_t MaxEntryKey() const { return entries_[Entries()-1].entry_key; }

  friend Test;

 private:
  struct __attribute__((aligned(64))) Entry {
    uint64_t entry_key;
    union {
      uint64_t meta;
      struct {
        uint64_t clevel       : 48;   // LSB
        uint64_t prefix_bytes : 4;
        uint64_t suffix_bytes : 4;
        uint64_t buf_entries  : 4;
        uint64_t max_entries  : 4;    // MSB
      };
    };
    uint8_t  buf[48+64];        // two stack: |key-->      <--value|

    Entry(uint64_t key, uint64_t value, int prefix_len);
    Entry(uint64_t key, int prefix_len);

    bool Put(uint64_t key, uint64_t value);
    bool Get(uint64_t key, uint64_t& value) const;
    bool Delete(uint64_t key, uint64_t* value);
    inline __attribute__((always_inline)) uint64_t Key(int index) const;
    inline __attribute__((always_inline)) uint64_t Value(int index) const;

    friend BLevel;
    friend Test;

   private:
    inline __attribute__((always_inline)) uint64_t KeyAt_(int index) const;
    inline __attribute__((always_inline)) void SetKey_(int index, uint64_t key);
    int BinarySearch_(uint64_t key, bool& find) const;
    inline __attribute__((always_inline)) uint8_t* key_(int index) const;
    inline __attribute__((always_inline)) uint64_t* value_(int index) const;
    bool WriteToCLevel_();
    void ClearBuf_();
    inline __attribute__((always_inline)) CLevel* clevel_() const;
    void CalcMaxEntries_() { max_entries = sizeof(buf) / (suffix_bytes + 8); }
  }; // Entry

  static_assert(sizeof(BLevel::Entry) == 128, "sizeof(BLevel::Entry) != 128");
  static_assert((sizeof(BLevel::Entry::buf) % 8) == 0, "BLevel::Entry::buf");

  // member
  uint64_t entries_offset_;                     // pmem file offset
  Entry* __attribute__((aligned(64))) entries_; // current mmaped address
  size_t nr_entries_;
  std::atomic<size_t> size_;

  // function
  uint64_t Find_(uint64_t key, uint64_t begin, uint64_t end) const;
};

}