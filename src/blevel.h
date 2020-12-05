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

    // FIXME: flush and fence?
    void SetInvalid() { buf.meta = 0; }
    bool IsValid()    { return buf.meta != 0; }

    void FlushToCLevel(CLevel::MemControl* mem);

    class Iter {
#ifdef BUF_SORT
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
#ifndef BUF_SORT
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
#ifndef BUF_SORT
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
        return (buf_idx_ >= entry_->buf.entries) && !point_to_clevel_;
      }

     private:
      const Entry* entry_;
      int buf_idx_;
      bool has_clevel_;
      bool point_to_clevel_;
      CLevel::Iter citer_;
#ifndef BUF_SORT
      int sorted_index_[16];
#endif

#undef entry_key
#undef entry_value
    };

    class NoSortIter {
     public:
      NoSortIter() {}

      NoSortIter(const Entry* entry, const CLevel::MemControl* mem)
        : entry_(entry), buf_idx_(0)
      {
        if (entry_->clevel.HasSetup()) {
          new (&citer_) CLevel::NoSortIter(&entry_->clevel, mem, entry_->entry_key);
          has_clevel_ = !citer_.end();
          point_to_clevel_ = has_clevel_ && (entry_->buf.entries == 0);
        } else {
          has_clevel_ = false;
          point_to_clevel_ = false;
        }
      }

      NoSortIter(const Entry* entry, const CLevel::MemControl* mem, uint64_t start_key)
        : entry_(entry), buf_idx_(0)
      {
        if (entry_->clevel.HasSetup()) {
          new (&citer_) CLevel::NoSortIter(&entry_->clevel, mem, entry_->entry_key, start_key);
          has_clevel_ = !citer_.end();
          point_to_clevel_ = has_clevel_ && (entry_->buf.entries == 0);
        } else {
          has_clevel_ = false;
          point_to_clevel_ = false;
        }
      }

      ALWAYS_INLINE uint64_t key() const {
        return point_to_clevel_ ? citer_.key() : entry_->key(buf_idx_);
      }

      ALWAYS_INLINE uint64_t value() const {
        return point_to_clevel_ ? citer_.value() : entry_->value(buf_idx_);
      }

      ALWAYS_INLINE bool next() {
        if (buf_idx_ < entry_->buf.entries - 1) {
          buf_idx_++;
          return true;
        } else if (buf_idx_ == entry_->buf.entries - 1) {
          buf_idx_++;
          if (has_clevel_) {
            point_to_clevel_ = true;
            return true;
          } else {
            return false;
          }
        } else if (point_to_clevel_) {
          point_to_clevel_ = citer_.next();
          return point_to_clevel_;
        } else {
          return false;
        }
      }

      ALWAYS_INLINE bool end() const {
        return (buf_idx_ >= entry_->buf.entries) && !point_to_clevel_;
      }

     private:
      const Entry* entry_;
      int buf_idx_;
      bool has_clevel_;
      bool point_to_clevel_;
      CLevel::NoSortIter citer_;
    };
  }; // Entry

  static_assert(sizeof(BLevel::Entry) == 128, "sizeof(BLevel::Entry) != 128");

 public:
  BLevel(size_t entries);
  ~BLevel();

  bool Put(uint64_t key, uint64_t value, uint64_t begin, uint64_t end);
  bool Get(uint64_t key, uint64_t& value, uint64_t begin, uint64_t end) const;
  bool Delete(uint64_t key, uint64_t* value, uint64_t begin, uint64_t end);

  bool PutRange(uint64_t key, uint64_t value, int range, uint64_t end);
  bool GetRange(uint64_t key, uint64_t& value, int range, uint64_t end) const;
  bool DeleteRange(uint64_t key, uint64_t* value, int range, uint64_t end);

  void Expansion(std::vector<std::pair<uint64_t,uint64_t>>& data);
#ifdef BRANGE
  bool IsKeyExpanded(uint64_t key, int& range, uint64_t& end) const;
  void PrepareExpansion(BLevel* old_blevel);
  void Expansion(BLevel* old_blevel);
#else
  void Expansion(BLevel* old_blevel);
#endif

  // statistic
  size_t CountCLevel() const;
  void PrefixCompression() const;
  int64_t CLevelTime() const;
  uint64_t Usage() const;

  ALWAYS_INLINE size_t Size() const { return size_; }
  ALWAYS_INLINE size_t Entries() const { return nr_entries_; }
  ALWAYS_INLINE uint64_t EntryKey(int logical_idx) const {
#ifdef BRANGE
    return entries_[GetPhysical_(logical_idx)].entry_key;
#else
    return entries_[logical_idx].entry_key;
#endif
  }

  class Iter {
   public:
    Iter(const BLevel* blevel)
      : blevel_(blevel), entry_idx_(0), range_end_(blevel->ranges_[0].entries),
        range_(0), locked_(false)
    {
#ifndef NO_LOCK
      blevel_->lock_[entry_idx_].lock_shared();
      locked_ = true;
      uint64_t last_idx = entry_idx_;
#endif
      new (&iter_) BLevel::Entry::Iter(&blevel_->entries_[entry_idx_], &blevel_->clevel_mem_);
      while (iter_.end() && NextIndex_()) {
#ifndef NO_LOCK
        blevel_->lock_[last_idx].unlock_shared();
        blevel_->lock_[entry_idx_].lock_shared();
        last_idx = entry_idx_;
#endif
        new (&iter_) BLevel::Entry::Iter(&blevel_->entries_[entry_idx_], &blevel_->clevel_mem_);
      }
      if (end()) {
        blevel_->lock_[entry_idx_].unlock_shared();
        locked_ = false;
      }
    }

    Iter(const BLevel* blevel, uint64_t start_key, uint64_t begin, uint64_t end)
      : blevel_(blevel), locked_(false)
    {
#ifdef BRANGE
      range_ = blevel_->FindBRangeByKey_(start_key);
      range_end_ = blevel_->ranges_[range_].physical_entry_start+blevel_->ranges_[range_].entries;
      end = std::min(blevel_->ranges_[range_+1].logical_entry_start-1, end);
      entry_idx_ = blevel_->FindByRange_(start_key, range_, blevel_->GetPhysical_(blevel_->ranges_[range_], end), nullptr);
#else
      entry_idx_ = blevel_->Find_(start_key, begin, end);
#endif
#ifndef NO_LOCK
      blevel_->lock_[entry_idx_].lock_shared();
      locked_ = true;
      uint64_t last_idx = entry_idx_;
#endif
      new (&iter_) BLevel::Entry::Iter(&blevel_->entries_[entry_idx_], &blevel_->clevel_mem_, start_key);
      while (iter_.end() && NextIndex_()) {
#ifndef NO_LOCK
        blevel_->lock_[last_idx].unlock_shared();
        blevel_->lock_[entry_idx_].lock_shared();
        last_idx = entry_idx_;
#endif
        new (&iter_) BLevel::Entry::Iter(&blevel_->entries_[entry_idx_], &blevel_->clevel_mem_, start_key);
      }
      if (this->end()) {
        blevel_->lock_[entry_idx_].unlock_shared();
        locked_ = false;
      }
    }

    ~Iter() {
      if (locked_)
        blevel_->lock_[entry_idx_].unlock_shared();
    }

    ALWAYS_INLINE uint64_t key() const {
      return iter_.key();
    }

    ALWAYS_INLINE uint64_t value() const {
      return iter_.value();
    }

    ALWAYS_INLINE bool next() {
      if (!iter_.next()) {
#ifndef NO_LOCK
        uint64_t last_idx = entry_idx_;
#endif
        while (iter_.end() && NextIndex_()) {
#ifndef NO_LOCK
          blevel_->lock_[last_idx].unlock_shared();
          blevel_->lock_[entry_idx_].lock_shared();
          last_idx = entry_idx_;
#endif
          new (&iter_) BLevel::Entry::Iter(&blevel_->entries_[entry_idx_], &blevel_->clevel_mem_);
        }
        if (end()) {
          blevel_->lock_[entry_idx_].unlock_shared();
          locked_ = false;
          return false;
        } else {
          return true;
        }
      } else {
        return true;
      }
    }

    ALWAYS_INLINE bool end() const {
#ifdef BRANGE
      return range_ >= EXPAND_THREADS;
#else
      return entry_idx_ >= blevel_->Entries();
#endif
    }

   private:
    ALWAYS_INLINE bool NextIndex_() {
#ifdef BRANGE
      if (++entry_idx_ < range_end_) {
        return true;
      } else {
        if (++range_ == EXPAND_THREADS)
          return false;
        entry_idx_ = blevel_->ranges_[range_].physical_entry_start;
        range_end_ = entry_idx_ + blevel_->ranges_[range_].entries;
        return true;
      }
#else
      return ++entry_idx_ < blevel_->Entries();
#endif
    }

    BLevel::Entry::Iter iter_;
    const BLevel* blevel_;
    uint64_t entry_idx_;
    uint64_t range_end_;
    int range_;
    bool locked_;
  };

  class NoSortIter {
   public:
    NoSortIter(const BLevel* blevel)
      : blevel_(blevel), entry_idx_(0), range_end_(blevel->ranges_[0].entries),
        range_(0), locked_(false)
    {
#ifndef NO_LOCK
      blevel_->lock_[entry_idx_].lock_shared();
      locked_ = true;
      uint64_t last_idx = entry_idx_;
#endif
      new (&iter_) BLevel::Entry::NoSortIter(&blevel_->entries_[entry_idx_], &blevel_->clevel_mem_);
      while (iter_.end() && NextIndex_()) {
#ifndef NO_LOCK
        blevel_->lock_[last_idx].unlock_shared();
        blevel_->lock_[entry_idx_].lock_shared();
        last_idx = entry_idx_;
#endif
        new (&iter_) BLevel::Entry::NoSortIter(&blevel_->entries_[entry_idx_], &blevel_->clevel_mem_);
      }
      if (end()) {
        blevel_->lock_[entry_idx_].unlock_shared();
        locked_ = false;
      }
    }

    NoSortIter(const BLevel* blevel, uint64_t start_key, uint64_t begin, uint64_t end)
      : blevel_(blevel), locked_(false)
    {
#ifdef BRANGE
      range_ = blevel_->FindBRangeByKey_(start_key);
      range_end_ = blevel_->ranges_[range_].physical_entry_start+blevel_->ranges_[range_].entries;
      end = std::min(blevel_->ranges_[range_+1].logical_entry_start-1, end);
      entry_idx_ = blevel_->FindByRange_(start_key, range_, blevel_->GetPhysical_(blevel_->ranges_[range_], end), nullptr);
#else
      entry_idx_ = blevel_->Find_(start_key, begin, end);
#endif
#ifndef NO_LOCK
      blevel_->lock_[entry_idx_].lock_shared();
      locked_ = true;
      uint64_t last_idx = entry_idx_;
#endif
      new (&iter_) BLevel::Entry::NoSortIter(&blevel_->entries_[entry_idx_], &blevel_->clevel_mem_, start_key);
      while (iter_.end() && NextIndex_()) {
#ifndef NO_LOCK
        blevel_->lock_[last_idx].unlock_shared();
        blevel_->lock_[entry_idx_].lock_shared();
        last_idx = entry_idx_;
#endif
        new (&iter_) BLevel::Entry::NoSortIter(&blevel_->entries_[entry_idx_], &blevel_->clevel_mem_, start_key);
      }
      if (this->end()) {
        blevel_->lock_[entry_idx_].unlock_shared();
        locked_ = false;
      }
    }

    ~NoSortIter() {
      if (locked_)
        blevel_->lock_[entry_idx_].unlock_shared();
    }

    ALWAYS_INLINE uint64_t key() const {
      return iter_.key();
    }

    ALWAYS_INLINE uint64_t value() const {
      return iter_.value();
    }

    ALWAYS_INLINE bool next() {
      if (!iter_.next()) {
#ifndef NO_LOCK
        uint64_t last_idx = entry_idx_;
#endif
        while (iter_.end() && NextIndex_()) {
#ifndef NO_LOCK
          blevel_->lock_[last_idx].unlock_shared();
          blevel_->lock_[entry_idx_].lock_shared();
          last_idx = entry_idx_;
#endif
          new (&iter_) BLevel::Entry::NoSortIter(&blevel_->entries_[entry_idx_], &blevel_->clevel_mem_);
        }
        if (end()) {
          blevel_->lock_[entry_idx_].unlock_shared();
          locked_ = false;
          return false;
        } else {
          return true;
        }
      } else {
        return true;
      }
    }

    ALWAYS_INLINE bool end() const {
#ifdef BRANGE
      return range_ >= EXPAND_THREADS;
#else
      return entry_idx_ >= blevel_->Entries();
#endif
    }

   private:
    ALWAYS_INLINE bool NextIndex_() {
#ifdef BRANGE
      if (++entry_idx_ < range_end_) {
        return true;
      } else {
        if (++range_ == EXPAND_THREADS)
          return false;
        entry_idx_ = blevel_->ranges_[range_].physical_entry_start;
        range_end_ = entry_idx_ + blevel_->ranges_[range_].entries;
        return true;
      }
#else
      return ++entry_idx_ < blevel_->Entries();
#endif
    }

    BLevel::Entry::NoSortIter iter_;
    const BLevel* blevel_;
    uint64_t entry_idx_;
    uint64_t range_end_;
    int range_;
    bool locked_;
  };

  friend Test;

#ifdef BRANGE
  static std::mutex expand_wait_lock;
  static std::condition_variable expand_wait_cv;
#endif

 private:
  struct ExpandData {
    Entry* new_addr;
    Entry* max_addr;
    uint64_t key_buf[BLEVEL_EXPAND_BUF_KEY];
    uint64_t value_buf[BLEVEL_EXPAND_BUF_KEY];
    uint64_t clevel_data_count;
    uint64_t clevel_count;
    uint64_t size;
#ifdef BRANGE
    uint64_t begin_range;
    uint64_t begin_interval;
    uint64_t end_range;
    uint64_t end_interval;
    uint64_t target_range;
#endif
    uint64_t entry_key;
    uint64_t last_entry_key;
    int buf_count;
    std::atomic<uint64_t>* max_key;
    std::atomic<uint64_t>* expanded_entries;

    ExpandData() = default;

    ExpandData(Entry* begin_addr, Entry* end_addr, uint64_t first_entry_key)
      : new_addr(begin_addr), max_addr(end_addr), clevel_data_count(0),
        clevel_count(0), size(0),
#ifdef BRANGE
        begin_range(0), begin_interval(0), end_range(0),
        end_interval(0), target_range(0),
#endif
        entry_key(first_entry_key), buf_count(0), max_key(nullptr), expanded_entries(nullptr) {}

    void FlushToEntry(Entry* entry, int prefix_len, CLevel::MemControl* mem);
  };

  // member
  void* pmem_addr_;
  size_t mapped_len_;
  std::string pmem_file_;
  static int file_id_;

  uint64_t entries_offset_;                     // pmem file offset
  Entry* __attribute__((aligned(64))) entries_; // current mmaped address
  size_t nr_entries_;                           // logical entries count
  size_t physical_nr_entries_;                  // physical entries count
  std::atomic<size_t> size_;
  CLevel::MemControl clevel_mem_;

#ifndef NO_LOCK
  std::shared_mutex* lock_;
#endif

#ifdef BRANGE
  struct BRange {
    uint64_t start_key;
    uint64_t logical_entry_start;
    uint64_t physical_entry_start;
    uint64_t entries;
  } ranges_[EXPAND_THREADS+1];

  // logical continuous interval, every interval contains interval_size_ entries,
  // size_per_interval_ contains kv-pair size per interval.
  uint64_t interval_size_;
  uint64_t intervals_[EXPAND_THREADS];
  mutable std::atomic<size_t>* size_per_interval_[EXPAND_THREADS];

  static ExpandData expand_data_[EXPAND_THREADS];
  static std::atomic<uint64_t> expanded_max_key_[EXPAND_THREADS];
  static std::atomic<uint64_t> expanded_entries_[EXPAND_THREADS];
#endif

  // function
#ifdef BRANGE
  ALWAYS_INLINE int FindBRange_(uint64_t logical_idx) const {
    for (int i = EXPAND_THREADS-1; i >= 0; --i)
      if (logical_idx >= ranges_[i].logical_entry_start)
        return i;
    assert(0);
  }

  ALWAYS_INLINE int FindBRangeByKey_(uint64_t key) const {
    for (int i = EXPAND_THREADS-1; i >= 0; --i)
      if (key >= ranges_[i].start_key)
        return i;
    assert(0);
  }

  ALWAYS_INLINE uint64_t GetPhysical_(const BRange& range, uint64_t logical_idx) const {
    // assert(logical_idx - range.logical_entry_start < range.entries);
    return range.physical_entry_start + (logical_idx - range.logical_entry_start);
  }

  ALWAYS_INLINE uint64_t GetPhysical_(uint64_t logical_idx) const {
    return GetPhysical_(ranges_[FindBRange_(logical_idx)], logical_idx);
  }

  ALWAYS_INLINE uint64_t GetLogical_(const BRange& range, uint64_t physical_idx) const {
    return range.logical_entry_start + (physical_idx - range.physical_entry_start);
  }
#endif

#ifdef BRANGE
  void ExpandRange_(BLevel* old_blevel, int thread_id);
  void FinishExpansion_();
  uint64_t Find_(uint64_t key, uint64_t begin, uint64_t end, std::atomic<size_t>** interval) const;
  uint64_t FindByRange_(uint64_t key, int range, uint64_t end, std::atomic<size_t>** interval) const;
#else
  uint64_t Find_(uint64_t key, uint64_t begin, uint64_t end) const;
#endif
  void ExpandSetup_(ExpandData& data);
  void ExpandPut_(ExpandData& data, uint64_t key, uint64_t value);
  void ExpandFinish_(ExpandData& data);

  ALWAYS_INLINE bool Put_(uint64_t key, uint64_t value, uint64_t physical_idx
#ifdef BRANGE
                                  , std::atomic<size_t>* interval_size
#endif
                                  ) {
    // assert(entries_[physical_idx].entry_key <= key);
#ifndef NO_LOCK
    std::lock_guard<std::shared_mutex> lock(lock_[physical_idx]);
#endif
    if (!entries_[physical_idx].IsValid())
      return false;
    entries_[physical_idx].Put(&clevel_mem_, key, value);
    size_++;
#ifdef BRANGE
    (*interval_size)++;
#endif
    return true;
  }

  ALWAYS_INLINE bool Get_(uint64_t key, uint64_t& value, uint64_t physical_idx) const {
#ifndef NO_LOCK
    std::shared_lock<std::shared_mutex> lock(lock_[physical_idx]);
#endif
    if (!entries_[physical_idx].IsValid())
      return false;
    return entries_[physical_idx].Get((CLevel::MemControl*)&clevel_mem_, key, value);
  }

  ALWAYS_INLINE bool Delete_(uint64_t key, uint64_t* value, uint64_t physical_idx
#ifdef BRANGE
                                  , std::atomic<size_t>* interval_size
#endif
                                  ) {
#ifndef NO_LOCK
    std::lock_guard<std::shared_mutex> lock(lock_[physical_idx]);
#endif
    if (!entries_[physical_idx].IsValid())
      return false;
    entries_[physical_idx].Delete(&clevel_mem_, key, value);
    size_--;
#ifdef BRANGE
    (*interval_size)--;
#endif
    return true;
  }
};

}