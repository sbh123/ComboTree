#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cassert>
#include "pmem.h"

namespace combotree {

template<const size_t buf_size, const size_t value_size = 8>
struct KVBuffer {
  union {
    uint16_t meta;
    struct {
      uint16_t prefix_bytes : 4;  // LSB
      uint16_t suffix_bytes : 4;
      uint16_t entries      : 4;
      uint16_t max_entries  : 4;  // MSB
    };
  };
  // | key 0 | key 1 | ... | key n-1 | .. | value n-1 | ... | value 1 | value 0 |
  uint8_t buf[buf_size];

  ALWAYS_INLINE const int MaxEntries() const { return buf_size / (value_size + suffix_bytes); }

  ALWAYS_INLINE bool Full() const { return entries >= max_entries; }

  ALWAYS_INLINE void* pkey(int idx) const {
    return (void*)&buf[idx*suffix_bytes];
  }

  ALWAYS_INLINE void* pvalue(int idx) const {
    return (void*)&buf[buf_size-(idx+1)*value_size];
  }

  ALWAYS_INLINE uint64_t key(int idx, uint64_t key_prefix) const {
    static uint64_t prefix_mask[9] = {
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
    static uint64_t suffix_mask[9] = {
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

    return (key_prefix & prefix_mask[prefix_bytes]) |
           ((*(uint64_t*)pkey(idx)) & suffix_mask[suffix_bytes]);

    // uint64_t ret = *(uint64_t*)pkey(idx);
    // memcpy(((uint8_t*)&ret)+suffix_bytes, ((uint8_t*)&key_prefix)+suffix_bytes, prefix_bytes);
    // return ret;
  }

  ALWAYS_INLINE uint64_t value(int idx) const {
    // the const bit mask will be generated during compile
    return *(uint64_t*)pvalue(idx) & (0xFFFFFFFFFFFFFFFFUL >> ((8-value_size)*8));
  }

  int Find(uint64_t target, bool& find) const {
    int left = 0;
    int right = entries - 1;
    while (left <= right) {
      int middle = (left + right) / 2;
      uint64_t mid_key = key(middle, target);
      if (mid_key == target) {
        find = true;
        return middle;
      } else if (mid_key > target) {
        right = middle - 1;
      } else {
        left = middle + 1;
      }
    }
    find = false;
    return left;
  }

  ALWAYS_INLINE void Clear() {
    entries = 0;
    flush(&meta);
    fence();
  }

  ALWAYS_INLINE bool Put(int pos, void* new_key, uint64_t value) {
    // TODO: flush and fence
    memmove(pkey(pos+1), pkey(pos), suffix_bytes*(entries-pos));
    memmove(pvalue(entries), pvalue(entries-1), value_size*(entries-pos));

    memcpy(pvalue(pos), &value, value_size);
    memcpy(pkey(pos), new_key, suffix_bytes);

    entries++;
    return true;
  }

  ALWAYS_INLINE bool Put(int pos, uint64_t new_key, uint64_t value) {
    return Put(pos, &new_key, value);
  }

  ALWAYS_INLINE bool Delete(int pos) {
    // TODO: flush and fence
    assert(pos < entries && pos >= 0);
    memmove(pkey(pos), pkey(pos+1), suffix_bytes*(entries-pos-1));
    memmove(pvalue(entries-2), pvalue(entries-1), value_size*(entries-pos-1));
    entries--;
    return true;
  }

  void MoveData(KVBuffer<buf_size, value_size>* dest, int start_pos, int entry_count) {
    // TODO: flush and fence
    memcpy(dest->pkey(0), pkey(start_pos), suffix_bytes*entry_count);
    memcpy(dest->pvalue(entry_count-1), pvalue(start_pos+entry_count-1), value_size*entry_count);
    entries -= entry_count;
    dest->entries = entry_count;
  }
};

}