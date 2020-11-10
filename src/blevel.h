#pragma once

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <vector>
#include <shared_mutex>
#include "combotree_config.h"
#include "kvbuffer.h"
#include "clevel.h"
#include "pmem.h"

namespace combotree {

class Test;

class BLevel {
 private:
  struct __attribute__((aligned(64))) Entry {
    uint64_t entry_key;
    CLevel clevel;
    KVBuffer<48+64,8> buf;  // contains 2 bytes meta

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

    class Iter {
#if BUF_SORT
#define entry_key(idx)    entry_->key(idx)
#define entry_value(idx)  entry_->value(idx)
#else
#define entry_key(idx)    entry_->key(sorted_index_[idx])
#define entry_value(idx)  entry_->value(sorted_index_[idx])
#endif
     public:
      Iter() {}

      Iter(const Entry* entry, const CLevel::MemControl* mem)
        : entry_(entry), buf_idx_(0)
      {
#if !defined(BUF_SORT) || BUF_SORT == 0
        entry->buf.GetSortedIndex(sorted_index_);
#endif
        if (entry_->clevel.HasSetup()) {
          new (&citer_) CLevel::Iter(&entry_->clevel, mem, entry_->entry_key);
          has_clevel_ = !citer_.end();
          point_to_clevel_ = has_clevel_ && (entry_->buf.entries == 0 || citer_.key() < entry_key(0));
        } else {
          has_clevel_ = false;
          point_to_clevel_ = false;
        }
      }

      Iter(const Entry* entry, const CLevel::MemControl* mem, uint64_t start_key)
        : entry_(entry), buf_idx_(0)
      {
#if !defined(BUF_SORT) || BUF_SORT == 0
        entry->buf.GetSortedIndex(sorted_index_);
#endif
        if (start_key <= entry->entry_key) {
          if (entry_->clevel.HasSetup()) {
            new (&citer_) CLevel::Iter(&entry_->clevel, mem, entry_->entry_key);
            has_clevel_ = !citer_.end();
            point_to_clevel_ = has_clevel_ && (entry_->buf.entries == 0 || citer_.key() < entry_key(0));
          } else {
            has_clevel_ = false;
            point_to_clevel_ = false;
          }
          return;
        } else if (entry_->clevel.HasSetup()) {
          new (&citer_) CLevel::Iter(&entry_->clevel, mem, entry_->entry_key, start_key);
          has_clevel_ = !citer_.end();
          point_to_clevel_ = has_clevel_ && (entry_->buf.entries == 0 || citer_.key() < entry_key(0));
        } else {
          has_clevel_ = false;
          point_to_clevel_ = false;
        }
        do {
          if (key() >= start_key)
            return;
        } while (next());
      }

      ALWAYS_INLINE uint64_t key() const {
        return point_to_clevel_ ? citer_.key() : entry_key(buf_idx_);
      }

      ALWAYS_INLINE uint64_t value() const {
        return point_to_clevel_ ? citer_.value() : entry_value(buf_idx_);
      }

      ALWAYS_INLINE bool next() {
        if (point_to_clevel_) {
          if (!citer_.next()) {
            has_clevel_ = false;
            point_to_clevel_ = false;
            return buf_idx_ < entry_->buf.entries;
          } else {
            point_to_clevel_ = buf_idx_ >= entry_->buf.entries ||
                               citer_.key() < entry_key(buf_idx_);
            return true;
          }
        } else if (has_clevel_) {
          buf_idx_++;
          point_to_clevel_ = buf_idx_ >= entry_->buf.entries ||
                             citer_.key() < entry_key(buf_idx_);
          return true;
        } else {
          buf_idx_++;
          return buf_idx_ < entry_->buf.entries;
        }
      }

      ALWAYS_INLINE bool end() const {
        return buf_idx_ >= entry_->buf.entries && !point_to_clevel_;
      }

     private:
      const Entry* entry_;
      int buf_idx_;
      bool has_clevel_;
      bool point_to_clevel_;
      CLevel::Iter citer_;
#if !defined(BUF_SORT) || BUF_SORT == 0
      int sorted_index_[16];
#endif
    };

#undef entry_key
#undef entry_value
  }; // Entry

  static_assert(sizeof(BLevel::Entry) == 128, "sizeof(BLevel::Entry) != 128");

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

  class Iter {
   public:
    Iter(const BLevel* blevel)
      : blevel_(blevel), entry_idx_(0)
    {
      do {
        new (&iter_) BLevel::Entry::Iter(&blevel_->entries_[entry_idx_], &blevel_->clevel_mem_);
      } while (iter_.end() && ++entry_idx_ < blevel_->Entries());
    }

    Iter(const BLevel* blevel, uint64_t start_key, uint64_t begin, uint64_t end)
      : blevel_(blevel)
    {
      entry_idx_ = blevel_->Find_(start_key, begin, end);
      do {
        new (&iter_) BLevel::Entry::Iter(&blevel_->entries_[entry_idx_], &blevel_->clevel_mem_, start_key);
      } while (iter_.end() && ++entry_idx_ < blevel_->Entries());
    }

    ALWAYS_INLINE uint64_t key() const {
      return iter_.key();
    }

    ALWAYS_INLINE uint64_t value() const {
      return iter_.value();
    }

    ALWAYS_INLINE bool next() {
      if (!iter_.next()) {
        while (iter_.end() && ++entry_idx_ < blevel_->Entries())
          new (&iter_) BLevel::Entry::Iter(&blevel_->entries_[entry_idx_], &blevel_->clevel_mem_);
        return entry_idx_ < blevel_->Entries();
      }
      return true;
    }

    ALWAYS_INLINE bool end() const {
      return entry_idx_ >= blevel_->Entries();
    }

   private:
    const BLevel* blevel_;
    uint64_t entry_idx_;
    BLevel::Entry::Iter iter_;
  };

  friend Test;

  struct ExpandData {
    Entry* new_addr;
    uint64_t key_buf[BLEVEL_EXPAND_BUF_KEY];
    uint64_t value_buf[BLEVEL_EXPAND_BUF_KEY];
    int buf_count;
    bool zero_entry;
    uint64_t clevel_data_count;
    uint64_t clevel_count;

    ExpandData(Entry* entries) {
      buf_count = 0;
      new_addr = entries;
      zero_entry = true;
      clevel_data_count = 0;
      clevel_count = 0;
    }

    void FlushToEntry(Entry* entry, int prefix_len, CLevel::MemControl* mem);
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
#ifndef NO_LOCK
  std::shared_mutex* lock_;
#endif

  // function
  uint64_t Find_(uint64_t key, uint64_t begin, uint64_t end) const;
  void ExpandSetup_(ExpandData& data);
  void ExpandPut_(ExpandData& data, uint64_t key, uint64_t value);
  void ExpandFinish_(ExpandData& data);
};

}