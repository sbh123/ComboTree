#pragma once

#include <atomic>
#include <cstdint>
#include <iostream>
#include <cstddef>
#include <vector>
#include <shared_mutex>
#include "combotree_config.h"
#include "helper.h"
#include "kvbuffer.h"
#include "clevel.h"
#include "pmem.h"

namespace combotree {

struct __attribute__((aligned(8))) NobufBEntry {
    uint64_t entry_key;
    CLevel clevel;
    union {
    uint16_t meta;
      struct {
        uint16_t prefix_bytes : 4;  // LSB
        uint16_t suffix_bytes : 4;
        uint16_t entries      : 4;
        uint16_t max_entries  : 4;  // MSB
      };
    };
    // KVBuffer<112,8> buf;  // contains 2 bytes meta
    NobufBEntry(uint64_t key, int prefix_len, CLevel::MemControl* mem = nullptr) {
      prefix_bytes = prefix_len;
      suffix_bytes = 8 - prefix_len;
      entry_key = key;
      clevel.Setup(mem, suffix_bytes);
    }

    NobufBEntry(uint64_t key, uint64_t value, int prefix_len, CLevel::MemControl* mem = nullptr)
    {
      prefix_bytes = prefix_len;
      suffix_bytes = 8 - prefix_len;
      entry_key = key;
      clevel.Setup(mem, suffix_bytes);
    }
    
    bool Put(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
      if (unlikely(!clevel.HasSetup())) {
        clevel.Setup(mem, suffix_bytes);
      }
      return clevel.Put(mem, key, value);
    }
    bool Update(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
      if (unlikely(!clevel.HasSetup())) {
        return false;
      }
      return clevel.Update(mem, key, value);
    }
    bool Get(CLevel::MemControl* mem, uint64_t key, uint64_t& value) const {
      if (unlikely(!clevel.HasSetup())) {
        return false;
      }
      return clevel.Get(mem, key, value);
    }

    bool Delete(CLevel::MemControl* mem, uint64_t key, uint64_t* value) {
      if (unlikely(!clevel.HasSetup())) {
        return false;
      }
      return clevel.Delete(mem, key, value);
    }

    // FIXME: flush and fence?
    void SetInvalid() { meta = 0; }
    bool IsValid()    { return meta != 0; }

    void FlushToCLevel(CLevel::MemControl* mem);

    class Iter {
     public:
      Iter() {}

      Iter(const NobufBEntry* entry, const CLevel::MemControl* mem)
        : entry_(entry)
      {
        if (entry_->clevel.HasSetup()) {
          new (&citer_) CLevel::Iter(&entry_->clevel, mem, entry_->entry_key);
          point_to_clevel_ = !citer_.end();
        } else {
          point_to_clevel_ = false;
        }
      }

      Iter(const NobufBEntry* entry, const CLevel::MemControl* mem, uint64_t start_key)
        : entry_(entry)
      {
        if (entry_->clevel.HasSetup()) {
          new (&citer_) CLevel::Iter(&entry_->clevel, mem, entry_->entry_key, start_key);
          point_to_clevel_ = !citer_.end();
          if(!point_to_clevel_) return;
        } else {
          point_to_clevel_ = false;
          return ;
        }
        do {
          if (key() >= start_key)
            return;
        } while (next());
      }

      ALWAYS_INLINE uint64_t key() const {
        return citer_.key();
      }

      ALWAYS_INLINE uint64_t value() const {
        return citer_.value();
      }

      ALWAYS_INLINE bool next() {
        if (point_to_clevel_ && citer_.next()) {
          return true;
        } else {
          point_to_clevel_ = false;
          return false;
        }
      }

      ALWAYS_INLINE bool end() const {
        return !point_to_clevel_;
      }

     private:
      const NobufBEntry* entry_;
      bool point_to_clevel_;
      CLevel::Iter citer_;
    };
  }; // NobufBEntry
  static_assert(sizeof(NobufBEntry) == 16);
  extern std::atomic<int64_t> clevel_time;
} // End of namespace combotree