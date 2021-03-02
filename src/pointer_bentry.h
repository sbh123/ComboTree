#pragma once

#include <atomic>
#include <cstdint>
#include <iostream>
#include <cstddef>
#include <vector>
#include <shared_mutex>
#include <cmath>
#include "combotree/combotree.h"
#include "combotree_config.h"
#include "bitops.h"
#include "nvm_alloc.h"
#include "common_time.h"
#include "kvbuffer.h"
#include "clevel.h"
#include "pmem.h"

namespace combotree {

// Less then 64 bits
static inline int Find_first_zero_bit(void *data, size_t bits)
{
    uint64_t bitmap = (*(uint64_t *)data);
    int pos = _tzcnt_u64(~bitmap);
    return pos > bits ? bits : pos;
}
// #define USET_PREFIX_COMPRESS

#ifdef USET_PREFIX_COMPRESS
template<const size_t bucket_size = 256, const size_t value_size = 8>
class Buncket { // without Buncket main key
    ALWAYS_INLINE size_t maxEntrys(int idx) const {
        return max_entries;
    }

    // ALWAYS_INLINE void* pkey(int idx) const {
    //   return (void*)&buf[idx*suffix_bytes];
    // }

    // ALWAYS_INLINE void* pvalue(int idx) const {
    //   return (void*)&buf[buf_size-(idx+1)*value_size];
    // }

    ALWAYS_INLINE void* pkey(int idx) const {
      return (void*)&buf[idx*(suffix_bytes+ value_size)];
    }

    ALWAYS_INLINE void* pvalue(int idx) const {
      return (void*)&buf[idx*(suffix_bytes+ value_size) + suffix_bytes];
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
      if(idx >= max_entries) idx = max_entries - 1;
      return (key_prefix & prefix_mask[prefix_bytes]) |
            ((*(uint64_t*)pkey(idx)) & suffix_mask[suffix_bytes]);
    }

    status PutBufKV(uint64_t new_key, uint64_t value, int &data_index) {
      int target_idx = find_first_zero_bit(bitmap, 40);
      if(entries >= max_entries) {
        return status::Full;
      }
      assert(pvalue(target_idx) > pkey(target_idx));
      memcpy(pkey(target_idx), &new_key, suffix_bytes);
      memcpy(pvalue(target_idx), &value, value_size);
      set_bit(target_idx, bitmap);
      clflush(pkey(target_idx));
    //   NVM::Mem_persist(pkey(target_idx), suffix_bytes + value_size);
      fence();
      data_index = target_idx;
      return status::OK;
    }

    status SetValue(int pos, uint64_t value) {
      memcpy(pvalue(pos), &value, value_size);
      clflush(pvalue(pos));
      fence();
      return status::OK;
    }

    status DeletePos(int pos) {
      clear_bit(pos, &bitmap);
      return status::OK;
    }

public:
    class Iter;

    Buncket(uint64_t key, int prefix_len) : main_key(key), prefix_bytes(prefix_len), 
        suffix_bytes(8 - prefix_len), entries(0), next_bucket(nullptr) {
        next_bucket = nullptr;
        max_entries = std::min(buf_size / (value_size + suffix_bytes), 40UL);
        // std::cout << "Max Entry size is:" <<  max_entries << std::endl;
        memset(bitmap, 0, 5);
    }

    Buncket(uint64_t key, uint64_t value, int prefix_len) : main_key(key), prefix_bytes(prefix_len), 
            suffix_bytes(8 - prefix_len), entries(0), next_bucket(nullptr) {
        next_bucket = nullptr;
        max_entries = std::min(buf_size / (value_size + suffix_bytes), 40UL);
        // std::cout << "Max Entry size is:" <<  max_entries << std::endl;
        Put(nullptr, key, value);
    }

    ~Buncket() {
        
    }

    status Load(uint64_t *keys, uint64_t *values, int count) {
        assert(entries == 0 && count < max_entries);

        for(int target_idx = 0; target_idx < count; target_idx ++) {
            assert(pvalue(target_idx) > pkey(target_idx));
            memcpy(pkey(target_idx), &keys[target_idx], suffix_bytes);
            memcpy(pvalue(target_idx), &values[count - target_idx - 1], value_size);
            set_bit(target_idx, bitmap);
            total_indexs[target_idx] = target_idx;
            entries ++;
        }
        NVM::Mem_persist(this, sizeof(*this));
        return status::OK;
    }

    status Expand_(CLevel::MemControl *mem, Buncket *&next, uint64_t &split_key, int &prefix_len) {
        // int expand_pos = entries / 2;
        next = new (mem->Allocate<Buncket>()) Buncket(key(entries / 2), prefix_bytes);
        // int idx = 0;
        split_key = key(entries / 2);
        prefix_len = prefix_bytes;
        for(int i = entries / 2; i < entries; i ++) {
            next->Put(nullptr, key(i), value(i));
        }
        next->next_bucket = this->next_bucket;
        clflush(next);

        this->next_bucket = next;
        fence();
        for(int i = entries / 2; i < entries; i ++) {
            DeletePos(total_indexs[i]);
        }
        entries = entries / 2;
        clflush(&header);
        fence();
        return status::OK;
    }

    Buncket *Next() {
        return next_bucket;
    }

    int Find(uint64_t target, bool& find) const {
        int left = 0;
        int right = entries - 1;
        while (left <= right) {
            int middle = (left + right) / 2;
            uint64_t mid_key = key(middle);
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
        // int len = entries;
        // int left = 0;
        // uint64_t mid_key;
        // while(len > 0) {
        //     int half = len >> 1;
        //     mid_key = key(left + half);
        //     if (mid_key == target) {
        //         find = true;
        //         return left + half;
        //     } else if (mid_key > target) {
        //         len = half;
        //     } else {
        //         left = left + half + 1;
        //         len = len - half - 1;
        //     }
        // }
        // // for(int i = 0; i < len ; i ++) {
        // //     mid_key = key(left);
        // //     if (mid_key >= target) {
        // //         find = mid_key == target;
        // //         break;
        // //     } 
        // //     left ++;
        // // }
        return left;
    }

    ALWAYS_INLINE uint64_t value(int idx) const {
      // the const bit mask will be generated during compile
      return *(uint64_t*)pvalue(total_indexs[idx]) & (UINT64_MAX >> ((8-value_size)*8));
    }

    ALWAYS_INLINE uint64_t key(int idx) const {
      return key(total_indexs[idx], main_key);
    }

    status Put(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
        status ret = status::OK;
        bool find = false;
        int idx = 0;
        // Common::timers["BLevel_times"].start();
        int pos = Find(key, find);
        if(find) {
            return status::Exist;
        }
        // Common::timers["BLevel_times"].end();

        // Common::timers["CLevel_times"].start();
        ret = PutBufKV(key, value, idx);
        if(ret != status::OK) {
            return ret;
        }
        entries ++;
        if(pos < entries - 1) {
            memmove(&total_indexs[pos + 1], &total_indexs[pos], entries - pos - 1);
        }
        total_indexs[pos] = idx;
        clflush(&header); 
        // Common::timers["CLevel_times"].end();
        return status::OK;
    }

    status Update(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
        bool find = false;
        int pos = Find(key, find);
        if(!find && this->value(pos) == 0) {
            // Show();
            return status::NoExist;
        }
        SetValue(total_indexs[pos], value);
        return status::OK;
    }

    status Get(CLevel::MemControl* mem, uint64_t key, uint64_t& value) const
    {
        bool find = false;
        int pos = Find(key, find);
        if(!find || this->value(pos)== 0) {
            // Show();
            return status::NoExist;
        }
        value = this->value(pos);
        return status::OK;
    }

    status Delete(CLevel::MemControl* mem, uint64_t key, uint64_t* value)
    {
        bool find = false;
        int pos = Find(key, find);
        // std::cout << "Find at pos:" << pos << std::endl;
        // for(int i = 0; i < entries; i++) {
        // std::cout << "Keys[" << i <<"]: " << this->key(i) << std::endl;
        // }
        if(!find || this->value(pos) == 0) {
            return status::NoExist;
        }
        std::cout << "Delete index:" << total_indexs[pos] << std::endl;
        DeletePos(total_indexs[pos]);
        if(pos < entries - 1) {
            memmove(&total_indexs[pos], &total_indexs[pos + 1], entries - pos - 1);
        }
        // std::cout << "Find at pos:" << pos << std::endl;
        // for(int i = 0; i < entries; i++) {
        //   std::cout << "Keys[" << i <<"]: " << this->key(i) << std::endl;
        // }
        entries --;
        if(value) {
            *value = this->value(pos);
        }
        clflush(&header);
        fence();
        return status::OK;
    }

    void Show() const {
        std::cout << "This: " << this << std::endl;
        for(int i = 0; i < entries; i ++) {
            std::cout << "key: " << key(i) << ", value: " << value(i) << std::endl;
        }
    }

    uint64_t EntryCount() const {
        return entries;
    }

    void SetInvalid() {  }
    bool IsValid()    { return false; }

private:
     // Frist 8 byte head 
    const static size_t buf_size = bucket_size - 48;
    uint64_t main_key;
    Buncket *next_bucket;
    union {
        uint64_t header;
        struct {
            uint16_t prefix_bytes : 4;  // LSB
            uint16_t suffix_bytes : 4;
            uint16_t entries      : 8;
            uint16_t max_entries  : 8;  // MSB
            uint8_t bitmap[5];
        };
    };
    uint8_t total_indexs[24];
    char buf[buf_size];

public:
    class Iter {
    public:
        Iter() {}

        Iter(const Buncket* bucket, uint64_t prefix_key, uint64_t start_key)
            : cur_(bucket), prefix_key(prefix_key)
        {
            if(unlikely(start_key <= prefix_key)) {
                idx_ = 0;
                return;
            } else {
                bool find = false;
                idx_ = cur_->Find(start_key, find);
            }
        }

        Iter(const Buncket* bucket, uint64_t prefix_key)
            : cur_(bucket), prefix_key(prefix_key)
        {
            idx_ = 0;
        }

        ALWAYS_INLINE uint64_t key() const {
            return cur_->key(idx_);
        }

        ALWAYS_INLINE uint64_t value() const {
            return cur_->value(idx_);
        }

        // return false if reachs end
        ALWAYS_INLINE bool next() {
            idx_++;
            if (idx_ >= cur_->entries) {
                return false;
            } else {
                return true;
            }
        }

        ALWAYS_INLINE bool end() const {
            return cur_ == nullptr ? true : (idx_ >= cur_->entries ? true : false);
        }

        bool operator==(const Iter& iter) const { return idx_ == iter.idx_ && cur_ == iter.cur_; }
        bool operator!=(const Iter& iter) const { return idx_ != iter.idx_ || cur_ != iter.cur_; }

    private:
        uint64_t prefix_key;
        const Buncket* cur_;
        int idx_;   // current index in node
    };

};

#else

template<const size_t bucket_size = 256, const size_t value_size = 8, const size_t key_size = 8,
        const size_t bitmap_size = 6, const size_t max_entry_count = 16>
class Buncket { // without Buncket main key
    ALWAYS_INLINE size_t maxEntrys(int idx) const {
        return max_entries;
    }

    ALWAYS_INLINE void* pkey(int idx) const {
      return (void*)&buf[idx*(value_size + key_size)];
    }

    ALWAYS_INLINE void* pvalue(int idx) const {
      return (void*)&buf[idx*(value_size + key_size) + key_size];
    }

    status PutBufKV(uint64_t new_key, uint64_t value, int &data_index) {
      int target_idx = find_first_zero_bit(bitmap, 40);
      if(entries >= max_entries) {
        return status::Full;
      }

      memcpy(pkey(target_idx), &new_key, key_size);
      memcpy(pvalue(target_idx), &value, value_size);
      set_bit(target_idx, bitmap);
      clflush(pkey(target_idx));
    //   NVM::Mem_persist(pkey(target_idx), suffix_bytes + value_size);
      fence();
      data_index = target_idx;
      return status::OK;
    }

    status SetValue(int pos, uint64_t value) {
      memcpy(pvalue(pos), &value, value_size);
      clflush(pvalue(pos));
      fence();
      return status::OK;
    }

    status DeletePos(int pos) {
      clear_bit(pos, &bitmap);
      return status::OK;
    }

public:
    class Iter;

    Buncket(uint64_t key, int prefix_len) : entries(0), next_bucket(nullptr) {
        next_bucket = nullptr;
        max_entries = std::min(buf_size / (value_size + key_size), max_entry_count);
        // std::cout << "Max Entry size is:" <<  max_entries << std::endl;
        memset(bitmap, 0, bitmap_size);
    }

    explicit Buncket(uint64_t key, uint64_t value, int prefix_len) : entries(0), next_bucket(nullptr) {
        next_bucket = nullptr;
        max_entries = std::min(buf_size / (value_size + key_size), max_entry_count);
        // std::cout << "Max Entry size is:" <<  max_entries << std::endl;
        memset(bitmap, 0, bitmap_size);
        Put(nullptr, key, value);
    }

    ~Buncket() {
        
    }

    status Load(uint64_t *keys, uint64_t *values, int count) {
        assert(entries == 0 && count < max_entries);

        for(int target_idx = 0; target_idx < count; target_idx ++) {
            assert(pvalue(target_idx) > pkey(target_idx));
            memcpy(pkey(target_idx), &keys[target_idx], key_size);
            memcpy(pvalue(target_idx), &values[count - target_idx - 1], value_size);
            set_bit(target_idx, bitmap);
            total_indexs[target_idx] = target_idx;
            entries ++;
        }
        NVM::Mem_persist(this, sizeof(*this));
        return status::OK;
    }

    status Expand_(CLevel::MemControl *mem, Buncket *&next, uint64_t &split_key, int &prefix_len) {
        // int expand_pos = entries / 2;
        next = new (mem->Allocate<Buncket>()) Buncket(key(entries / 2), prefix_len);
        // int idx = 0;
        split_key = key(entries / 2);
        prefix_len = 0;
        for(int i = entries / 2; i < entries; i ++) {
            next->Put(nullptr, key(i), value(i));
        }
        next->next_bucket = this->next_bucket;
        clflush(next);

        this->next_bucket = next;
        fence();
        for(int i = entries / 2; i < entries; i ++) {
            DeletePos(total_indexs[i]);
        }
        entries = entries / 2;
        clflush(&header);
        fence();
        return status::OK;
    }

    Buncket *Next() {
        return next_bucket;
    }

    int Find(uint64_t target, bool& find) const {
        int left = 0;
        int right = entries - 1;
        while (left <= right) {
            int middle = (left + right) / 2;
            uint64_t mid_key = key(middle);
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

    ALWAYS_INLINE uint64_t value(int idx) const {
      // the const bit mask will be generated during compile
      return *(uint64_t*)pvalue(total_indexs[idx]);
    }

    ALWAYS_INLINE uint64_t key(int idx) const {
      return *(uint64_t*)pkey(total_indexs[idx]);
    }

    status Put(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
        status ret = status::OK;
        bool find = false;
        int idx = 0;
        int pos = Find(key, find);
        if(find) {
            return status::Exist;
        }
        ret = PutBufKV(key, value, idx);
        if(ret != status::OK) {
            return ret;
        }
        entries ++;
        if(pos < entries - 1) {
            memmove(&total_indexs[pos + 1], &total_indexs[pos], entries - pos - 1);
        }
        total_indexs[pos] = idx;
        clflush(&header); 
        return status::OK;
    }

    status Update(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
        bool find = false;
        int pos = Find(key, find);
        if(!find || this->value(pos) == 0) {
            // Show();
            return status::NoExist;
        }
        SetValue(total_indexs[pos], value);
        return status::OK;
    }

    status Get(CLevel::MemControl* mem, uint64_t key, uint64_t& value) const
    {
        bool find = false;
        int pos = Find(key, find);
        if(!find || this->value(pos)== 0) {
            // Show();
            return status::NoExist;
        }
        value = this->value(pos);
        return status::OK;
    }

    status Delete(CLevel::MemControl* mem, uint64_t key, uint64_t* value)
    {
        bool find = false;
        int pos = Find(key, find);
        // std::cout << "Find at pos:" << pos << std::endl;
        // for(int i = 0; i < entries; i++) {
        // std::cout << "Keys[" << i <<"]: " << this->key(i) << std::endl;
        // }
        if(!find || this->value(pos) == 0) {
            return status::NoExist;
        }
        std::cout << "Delete index:" << total_indexs[pos] << std::endl;
        DeletePos(total_indexs[pos]);
        if(pos < entries - 1) {
            memmove(&total_indexs[pos], &total_indexs[pos + 1], entries - pos - 1);
        }
        // std::cout << "Find at pos:" << pos << std::endl;
        // for(int i = 0; i < entries; i++) {
        //   std::cout << "Keys[" << i <<"]: " << this->key(i) << std::endl;
        // }
        entries --;
        if(value) {
            *value = this->value(pos);
        }
        clflush(&header);
        fence();
        return status::OK;
    }

    void Show() const {
        std::cout << "This: " << this << std::endl;
        for(int i = 0; i < entries; i ++) {
            std::cout << "key: " << key(i) << ", value: " << value(i) << std::endl;
        }
    }

    uint64_t EntryCount() const {
        return entries;
    }

    void SetInvalid() {  }
    bool IsValid()    { return false; }

private:
     // Frist 8 byte head 
    const static size_t buf_size = bucket_size - (16 + max_entry_count);
    static_assert(bitmap_size * 8 >=  max_entry_count);

    Buncket *next_bucket;
    union {
        uint64_t header;
        struct {
            uint16_t entries      : 8;
            uint16_t max_entries  : 8;  // MSB
            uint8_t bitmap[bitmap_size];
        };
    };
    uint8_t total_indexs[max_entry_count];
    char buf[buf_size];

public:
    class Iter {
    public:
        Iter() {}

        Iter(const Buncket* bucket, uint64_t prefix_key, uint64_t start_key)
            : cur_(bucket), prefix_key(prefix_key)
        {
            if(unlikely(start_key <= prefix_key)) {
                idx_ = 0;
                return;
            } else {
                bool find = false;
                idx_ = cur_->Find(start_key, find);
            }
        }

        Iter(const Buncket* bucket, uint64_t prefix_key)
            : cur_(bucket), prefix_key(prefix_key)
        {
            idx_ = 0;
        }

        ALWAYS_INLINE uint64_t key() const {
            return cur_->key(idx_);
        }

        ALWAYS_INLINE uint64_t value() const {
            return cur_->value(idx_);
        }

        // return false if reachs end
        ALWAYS_INLINE bool next() {
            idx_++;
            if (idx_ >= cur_->entries) {
                return false;
            } else {
                return true;
            }
        }

        ALWAYS_INLINE bool end() const {
            return cur_ == nullptr ? true : (idx_ >= cur_->entries ? true : false);
        }

        bool operator==(const Iter& iter) const { return idx_ == iter.idx_ && cur_ == iter.cur_; }
        bool operator!=(const Iter& iter) const { return idx_ != iter.idx_ || cur_ != iter.cur_; }

    private:
        uint64_t prefix_key;
        const Buncket* cur_;
        int idx_;   // current index in node
    };
};

#endif

/**
 * @brief 有序的C层节点，
 * 
 * @tparam bucket_size 
 * @tparam value_size 
 * @tparam key_size 
 * @tparam max_entry_count 
 */
template<const size_t bucket_size = 256, const size_t value_size = 8, const size_t key_size = 8,
    const size_t max_entry_count = 64>
class __attribute__((aligned(64))) SortBuncket { // without SortBuncket main key
    struct entry;

    ALWAYS_INLINE size_t maxEntrys(int idx) const {
        return max_entries;
    }

    ALWAYS_INLINE void* pkey(int idx) const {
    //   return (void*)&buf[idx*(value_size + key_size)];
        return (void *)&records[idx].key;
    }

    ALWAYS_INLINE void* pvalue(int idx) const {
    //   return (void*)&buf[idx*(value_size + key_size) + key_size];
        return (void *)&records[idx].ptr;
    }

    /**
     * @brief 插入一个KV对，仿照FastFair写的，先移动指针，再移动key
     * 
     * @param new_key 
     * @param value 
     * @param data_index 
     * @param flush 
     * @return status 
     */
    status PutBufKV(uint64_t new_key, uint64_t value, int &data_index, bool flush = true) {
      if(entries >= max_entries) {
        return status::Full;
      }
      if(entries == 0) {  // this page is empty
          records[0].key = new_key;
          records[0].ptr = value;
          records[1].ptr = 0;
          fence();
          return status::OK;
      }
      {
        int i = entries, inserted = 0;
        if(entries < max_entries - 1) {
            records[i+1].ptr = records[i].ptr; 
            if((uint64_t)pvalue(i+1) % CACHE_LINE_SIZE == 0) {
                if(flush) clflush(pvalue(i+1));
            }
        }

        for(i = entries - 1; i >= 0; i --) {
            if(new_key < records[i].key ) {
                records[i+1].ptr = records[i].ptr;
                records[i+1].key = records[i].key;
                if(!flush) continue;
                uint64_t records_ptr = (uint64_t)(&records[i+1]);
                int remainder = records_ptr % CACHE_LINE_SIZE;
                bool do_flush = (remainder == 0) || 
                    ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1) 
                    && ((remainder+sizeof(entry))%CACHE_LINE_SIZE)!=0);
                if(do_flush) {
                    clflush((char*)records_ptr);
                }
            } else {
                records[i+1].ptr = records[i].ptr;
                records[i+1].key = new_key;
                records[i+1].ptr = value;
                if(flush) clflush((char*)&records[i+1]);
                inserted = 1;
                break;
            }  
        }
        if(inserted == 0) {
            records[0].key = new_key;
            records[0].ptr = value;
            if(flush) clflush((char*) &records[0]); 
        }
      }
      fence();
      return status::OK;
    }

    /**
     * @brief 删除一个key，仿照FastFair写的，先移动指针，再移动key
     * 
     * @param key 
     * @param value 
     * @return true 
     * @return false 
     */
    inline bool remove_key(uint64_t key, uint64_t *value) {
      bool shift = false;
      int i;
      for(i = 0; records[i].ptr != 0; ++i) {
        if(!shift && records[i].key == key) {
          if(value) *value = records[i].ptr;
          if(i != 0) {
            records[i].ptr = records[i - 1].ptr;
          }
          shift = true;
        }

        if(shift) {
          records[i].key = records[i + 1].key;
          records[i].ptr = records[i + 1].ptr;
          uint64_t records_ptr = (uint64_t)(&records[i]);
          int remainder = records_ptr % CACHE_LINE_SIZE;
          bool do_flush = (remainder == 0) || 
            ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1) && 
             ((remainder + sizeof(entry)) % CACHE_LINE_SIZE) != 0);
          if(do_flush) {
            clflush((char *)records_ptr);
          }
        }
      }
      return shift;
    }

    status SetValue(int pos, uint64_t value) {
      memcpy(pvalue(pos), &value, value_size);
      clflush(pvalue(pos));
      fence();
      return status::OK;
    }

public:
    class Iter;

    SortBuncket(uint64_t key, int prefix_len) : entries(0), next_bucket(nullptr) {
        next_bucket = nullptr;
        max_entries = std::min(buf_size / (value_size + key_size), max_entry_count);
        // std::cout << "Max Entry size is:" <<  max_entries << std::endl;
    }

    explicit SortBuncket(uint64_t key, uint64_t value, int prefix_len) : entries(0), next_bucket(nullptr) {
        next_bucket = nullptr;
        max_entries = std::min(buf_size / (value_size + key_size), max_entry_count);
        // std::cout << "Max Entry size is:" <<  max_entries << std::endl;
        Put(nullptr, key, value);
    }

    ~SortBuncket() {
        
    }

    status Load(uint64_t *keys, uint64_t *values, int count) {
        assert(entries == 0 && count < max_entries);

        for(int target_idx = 0; target_idx < count; target_idx ++) {
            assert(pvalue(target_idx) > pkey(target_idx));
            memcpy(pkey(target_idx), &keys[target_idx], key_size);
            memcpy(pvalue(target_idx), &values[count - target_idx - 1], value_size);
            entries ++;
        }
        NVM::Mem_persist(this, sizeof(*this));
        return status::OK;
    }

    /**
     * @brief 节点分裂
     * 
     * @param mem 
     * @param next 
     * @param split_key 
     * @param prefix_len 
     * @return status 
     */
    status Expand_(CLevel::MemControl *mem, SortBuncket *&next, uint64_t &split_key, int &prefix_len) {
        // int expand_pos = entries / 2;
        next = new (mem->Allocate<SortBuncket>()) SortBuncket(key(entries / 2), prefix_len);
        // int idx = 0;
        split_key = key(entries / 2);
        prefix_len = 0;
        int idx = 0;
        int m = (int) ceil(entries / 2);
        for(int i = m; i < entries; i ++) {
            // next->Put(nullptr, key(i), value(i));
            next->PutBufKV(key(i), value(i), idx, false);
            next->entries ++;
        }
        next->next_bucket = this->next_bucket;
        NVM::Mem_persist(next, sizeof(*next));

        records[m].ptr = 0; 
        clflush(&records[entries / 2].ptr);

        this->next_bucket = next;
        fence();
        entries = m;
        clflush(&header);
        fence();
        return status::OK;
    }

    SortBuncket *Next() {
        return next_bucket;
    }

    int Find(uint64_t target, bool& find) const {
        int left = 0;
        int right = entries - 1;
        while (left <= right) {
            int middle = (left + right) / 2;
            uint64_t mid_key = key(middle);
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

    ALWAYS_INLINE uint64_t value(int idx) const {
      // the const bit mask will be generated during compile
      return records[idx].ptr;
    }

    ALWAYS_INLINE uint64_t key(int idx) const {
      return records[idx].key;
    }

    status Put(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
        status ret = status::OK;
        int idx = 0;
        // Common::timers["CLevel_times"].start();
        ret = PutBufKV(key, value, idx);
        if(ret != status::OK) {
            return ret;
        }
        entries ++;
        clflush(&header); 
        // Common::timers["CLevel_times"].end();
        return status::OK;
    }

    status Update(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
        bool find = false;
        int pos = Find(key, find);
        if(!find || this->value(pos) == 0) {
            // Show();
            return status::NoExist;
        }
        SetValue(pos, value);
        return status::OK;
    }

    status Get(CLevel::MemControl* mem, uint64_t key, uint64_t& value) const
    {
        bool find = false;
        int pos = Find(key, find);
        if(!find || this->value(pos)== 0) {
            // Show();
            return status::NoExist;
        }
        value = this->value(pos);
        return status::OK;
    }

    status Delete(CLevel::MemControl* mem, uint64_t key, uint64_t* value)
    {
        auto ret = remove_key(key, value);
        if(!ret) {
            return status::NoExist;
        }
        fence();
        entries --;
        clflush(&header);
        fence();
        return status::OK;
    }

    void Show() const {
        std::cout << "This: " << this << ", entry count: " << entries << std::endl;
        for(int i = 0; i < entries; i ++) {
            std::cout << "key: " << key(i) << ", value: " << value(i) << std::endl;
        }
    }

    uint64_t EntryCount() const {
        return entries;
    }

    void SetInvalid() {  }
    bool IsValid()    { return false; }

private:
     // Frist 8 byte head 
    struct entry {
        uint64_t key;
        uint64_t ptr;
    };

    const static size_t buf_size = bucket_size - (8 + 2);
    const static size_t entry_size = (key_size + value_size);
    const static size_t entry_count = (buf_size / entry_size);

    SortBuncket *next_bucket;
    union {
        uint16_t header;
        struct {
            uint16_t entries      : 8;
            uint16_t max_entries  : 8;  // MSB
        };
    };
    // char buf[buf_size];
    entry records[entry_count];

public:
    class Iter {
    public:
        Iter() {}

        Iter(const SortBuncket* bucket, uint64_t prefix_key, uint64_t start_key)
            : cur_(bucket), prefix_key(prefix_key)
        {
            if(unlikely(start_key <= prefix_key)) {
                idx_ = 0;
                return;
            } else {
                bool find = false;
                idx_ = cur_->Find(start_key, find);
            }
        }

        Iter(const SortBuncket* bucket, uint64_t prefix_key)
            : cur_(bucket), prefix_key(prefix_key)
        {
            idx_ = 0;
        }

        ALWAYS_INLINE uint64_t key() const {
            return cur_->key(idx_);
        }

        ALWAYS_INLINE uint64_t value() const {
            return cur_->value(idx_);
        }

        // return false if reachs end
        ALWAYS_INLINE bool next() {
            idx_++;
            if (idx_ >= cur_->entries) {
                return false;
            } else {
                return true;
            }
        }

        ALWAYS_INLINE bool end() const {
            return cur_ == nullptr ? true : (idx_ >= cur_->entries ? true : false);
        }

        bool operator==(const Iter& iter) const { return idx_ == iter.idx_ && cur_ == iter.cur_; }
        bool operator!=(const Iter& iter) const { return idx_ != iter.idx_ || cur_ != iter.cur_; }

    private:
        uint64_t prefix_key;
        const SortBuncket* cur_;
        int idx_;   // current index in node
    };
};

template<const size_t bucket_size = 256, const size_t value_size = 8, const size_t key_size = 8,
    const size_t max_entry_count = 64>
class __attribute__((aligned(64))) UnSortBuncket { 
    struct entry;

    ALWAYS_INLINE size_t maxEntrys(int idx) const {
        return max_entries;
    }

    ALWAYS_INLINE void* pkey(int idx) const {
    //   return (void*)&buf[idx*(value_size + key_size)];
        return (void *)&records[idx].key;
    }

    ALWAYS_INLINE void* pvalue(int idx) const {
    //   return (void*)&buf[idx*(value_size + key_size) + key_size];
        return (void *)&records[idx].ptr;
    }

    status PutBufKV(uint64_t new_key, uint64_t value, int &data_index, bool flush = true) {
      if(entries >= max_entries) {
        return status::Full;
      }
      records[entries].key = new_key;
      records[entries].ptr = value;
      if(flush) clflush((char*)&records[entries]);
      fence();
      return status::OK;
    }

    inline bool remove_key(uint64_t key, uint64_t *value) {
      bool find = false;
      int pos = Find(key, find);
      if(find) {
        if(pos == entries - 1) return true;
        else {
          if(value) *value = records[pos].ptr;
          records[pos].key = records[entries-1].key;
          records[pos].ptr = records[entries-1].ptr;
          return true;
        }
      }
      return false;
    }


    status SetValue(int pos, uint64_t value) {
      memcpy(pvalue(pos), &value, value_size);
      clflush(pvalue(pos));
      fence();
      return status::OK;
    }

    int getSortedIndex(int sorted_index[]) const {
        uint64_t keys[entries];
        for (int i = 0; i < entries; ++i) {
            keys[i] = key(i);  // prefix does not matter
            sorted_index[i] = i;
        }
        std::sort(&sorted_index[0], &sorted_index[entries],
            [&keys](uint64_t a, uint64_t b) { return keys[a] < keys[b]; });
        return entries;
    }

public:
    class Iter;

    UnSortBuncket(uint64_t key, int prefix_len) : entries(0), next_bucket(nullptr) {
        next_bucket = nullptr;
        max_entries = std::min(buf_size / (value_size + key_size), max_entry_count);
        // std::cout << "Max Entry size is:" <<  max_entries << std::endl;
    }

    explicit UnSortBuncket(uint64_t key, uint64_t value, int prefix_len) : entries(0), next_bucket(nullptr) {
        next_bucket = nullptr;
        max_entries = std::min(buf_size / (value_size + key_size), max_entry_count);
        // std::cout << "Max Entry size is:" <<  max_entries << std::endl;
        Put(nullptr, key, value);
    }

    ~UnSortBuncket() {
        
    }

    status Load(uint64_t *keys, uint64_t *values, int count) {
        assert(entries == 0 && count < max_entries);

        for(int target_idx = 0; target_idx < count; target_idx ++) {
            assert(pvalue(target_idx) > pkey(target_idx));
            memcpy(pkey(target_idx), &keys[target_idx], key_size);
            memcpy(pvalue(target_idx), &values[count - target_idx - 1], value_size);
            entries ++;
        }
        NVM::Mem_persist(this, sizeof(*this));
        return status::OK;
    }

    status Expand_(CLevel::MemControl *mem, UnSortBuncket *&next, uint64_t &split_key, int &prefix_len) {
        // int expand_pos = entries / 2;
        int sorted_index_[16];
        // std::cout << "expand call getSortedIndex" << std::endl;
        getSortedIndex(sorted_index_);
        split_key = key(sorted_index_[entries / 2]);
        //std::cout << "entries:" << entries << std::endl;
        // for(int i = 0; i < entries; i++) {
           // std::cout << "key(" << i << "):" << key(i) << std::endl;
           // std::cout << "Sorted_index_[" << i << "]):" << sorted_index_[i] << std::endl;
        //}
        if(key(sorted_index_[0]) >= split_key) {
            std::cout << "split_key is not the middle key" << std::endl;
        }
        assert(key(sorted_index_[0]) < split_key);
        next = new (mem->Allocate<UnSortBuncket>()) UnSortBuncket(split_key, prefix_len);
        // int idx = 0;
        prefix_len = 0;
        int idx = 0;
        for(int i = entries/2; i < entries; i ++) {
            // next->Put(nullptr, key(i), value(i));
            next->PutBufKV(key(sorted_index_[i]), value(sorted_index_[i]), idx, false);
            next->entries ++;
            clflush(&next->header);
        }
        next->next_bucket = this->next_bucket;
        NVM::Mem_persist(next, sizeof(*next));
        int m = entries;
        for(int i = entries/2; i < m; i++) {
            uint64_t *value_;
            remove_key(key(sorted_index_[i]), value_);
            fence();
            entries--;
            clflush(&header);
        }
        this->next_bucket = next;
        fence();
        return status::OK;
    }

    UnSortBuncket *Next() {
        return next_bucket;
    }

    int Find(uint64_t target, bool& find) const {
        for(int i = 0; i < entries; i++) {
            if(key(i) == target) {
                find = true;
                return i;
            }
        }
        find = false;
        return entries;
    }

    ALWAYS_INLINE uint64_t value(int idx) const {
      // the const bit mask will be generated during compile
      return records[idx].ptr;
    }

    ALWAYS_INLINE uint64_t key(int idx) const {
      return records[idx].key;
    }

    status Put(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
        status ret = status::OK;
        int idx = 0;
        // Common::timers["CLevel_times"].start();
        ret = PutBufKV(key, value, idx);
        if(ret != status::OK) {
            return ret;
        }
        entries ++;
        clflush(&header); 
        // Common::timers["CLevel_times"].end();
        return status::OK;
    }

    status Update(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
        bool find = false;
        int pos = Find(key, find);
        if(!find) {
            // Show();
            return status::NoExist;
        }
        SetValue(pos, value);
        return status::OK;
    }

    status Get(CLevel::MemControl* mem, uint64_t key, uint64_t& value) const
    {
        bool find = false;
        int pos = Find(key, find);
        if(!find) {
            // Show();
            return status::NoExist;
        }
        value = this->value(pos);
        return status::OK;
    }

    status Delete(CLevel::MemControl* mem, uint64_t key, uint64_t* value)
    {
        auto ret = remove_key(key, value);
        if(!ret) {
            return status::NoExist;
        }
        fence();
        entries --;
        clflush(&header);
        fence();
        return status::OK;
    }

    void Show() const {
        std::cout << "This: " << this << ", entry count: " << entries << std::endl;
        for(int i = 0; i < entries; i ++) {
            std::cout << "key: " << key(i) << ", value: " << value(i) << std::endl;
        }
    }

    uint64_t EntryCount() const {
        return entries;
    }

    void SetInvalid() {  }
    bool IsValid()    { return false; }

private:
     // Frist 8 byte head 
    struct entry {
        uint64_t key;
        uint64_t ptr;
    };

    const static size_t buf_size = bucket_size - (8 + 2);
    const static size_t entry_size = (key_size + value_size);
    const static size_t entry_count = (buf_size / entry_size);

    UnSortBuncket *next_bucket;
    union {
        uint16_t header;
        struct {
            uint16_t entries      : 8;
            uint16_t max_entries  : 8;  // MSB
        };
    };
    // char buf[buf_size];
    entry records[entry_count];

public:
    class Iter {
    public:
        Iter() {}

        Iter(const UnSortBuncket* bucket, uint64_t prefix_key, uint64_t start_key)
            : cur_(bucket), prefix_key(prefix_key)
        {
           // std::cout << "iter call getSortedIndex" << std::endl;
            cur_->getSortedIndex(sorted_index_);
            if(unlikely(start_key <= prefix_key)) {
                idx_ = 0;
                return;
            } else {
                for(int i = 0; i < cur_->entries; i++) {
                    if(cur_->key(sorted_index_[i]) >= start_key) {
                        idx_ = i;
                        return;
                    }
                }
                idx_ = cur_->entries;
                return;
            }
        }

        Iter(const UnSortBuncket* bucket, uint64_t prefix_key)
            : cur_(bucket), prefix_key(prefix_key)
        {
            // std::cout << "iter2 call getSortedIndex" << std::endl;
            cur_->getSortedIndex(sorted_index_);
            idx_ = 0;
        }

        ALWAYS_INLINE uint64_t key() const {
            if (idx_ < cur_->entries)
                return cur_->key(sorted_index_[idx_]);
            else return 0;
        }

        ALWAYS_INLINE uint64_t value() const {
            if (idx_ < cur_->entries)
                return cur_->value(sorted_index_[idx_]);
            else return 0;
        }

        // return false if reachs end
        ALWAYS_INLINE bool next() {
            if (idx_ >= cur_->entries - 1) {
                return false;
            } else {
                idx_ ++;
                return true;
            }
        }

        ALWAYS_INLINE bool end() const {
            return cur_ == nullptr ? true : (idx_ >= cur_->entries ? true : false);
        }

        bool operator==(const Iter& iter) const { return idx_ == iter.idx_ && cur_ == iter.cur_; }
        bool operator!=(const Iter& iter) const { return idx_ != iter.idx_ || cur_ != iter.cur_; }

    private:
        uint64_t prefix_key;
        const UnSortBuncket* cur_;
        int idx_;   // current index in sorted_index_
        int sorted_index_[16];
    };
};

// typedef Buncket<256, 8> buncket_t;
typedef UnSortBuncket<256, 8> buncket_t;  
// c层节点的定义， C层节点需支持Put，Get，Update，Delete
// C层节点内部需要实现一个Iter作为迭代器，

static_assert(sizeof(Buncket<256, 8>) == 256);

class __attribute__((packed)) BuncketPointer {

#define READ_SIX_BYTE(addr) ((*(uint64_t*)addr) & 0x0000FFFFFFFFFFFFUL)
    uint8_t pointer_[6]; // uint48_t, LSB == 1 means NULL
public:
    BuncketPointer() { pointer_[0] = 1;}
    
    ALWAYS_INLINE bool HasSetup() const { return !(pointer_[0] & 1); };

    void Setup(CLevel::MemControl* mem, uint64_t key, int prefix_len) {
        buncket_t *buncket = new (mem->Allocate<buncket_t>()) buncket_t(key, prefix_len);
        uint64_t pointer = (uint64_t)(buncket) - mem->BaseAddr();
        memcpy(pointer_, &pointer, sizeof(pointer_));
    }

    void Setup(CLevel::MemControl* mem, buncket_t *buncket, uint64_t key, int prefix_len) {
        uint64_t pointer = (uint64_t)(buncket) - mem->BaseAddr();
        memcpy(pointer_, &pointer, sizeof(pointer_));
    }

    ALWAYS_INLINE buncket_t *pointer(uint64_t base_addr) const {
        return (buncket_t *) (READ_SIX_BYTE(pointer_) + base_addr);
    }
};

/**
 * @brief B 层数组，包括4个C层节点，entry_count 可以控制C层节点个数
 * @ BuncketPointer C层节点指针的封装，仿照Combotree以6bit的偏移代替8字节的指针
 * 2个字节的buf头部，只有第一个的entries 在用，其他都感觉没有在用，可以改进
 */
struct  PointerBEntry {
    static const int entry_count = 4;
    struct entry {
        uint64_t entry_key;
        BuncketPointer pointer;
        union {
            uint16_t meta;
            struct {
                uint16_t prefix_bytes : 4;  // LSB
                uint16_t suffix_bytes : 4;
                uint16_t entries      : 4;
                uint16_t max_entries  : 4;  // MSB
            };
        } buf;
        uint64_t DataCount(const CLevel::MemControl *mem)  const {
            return pointer.pointer(mem->BaseAddr())->EntryCount();
        }
        void SetInvalid() { buf.meta = 0; }
        bool IsValid()    const { return buf.meta != 0; }
    };
    union {
        struct {
            uint64_t entry_key;
            char pointer[6];
            union {
                uint16_t meta;
                struct {
                    uint16_t prefix_bytes : 4;  // LSB
                    uint16_t suffix_bytes : 4;
                    uint16_t entries      : 4;
                    uint16_t max_entries  : 4;  // MSB
                };
            } buf;
        };
        struct entry entrys[entry_count];
    };


    ALWAYS_INLINE buncket_t *Pointer(int i, const CLevel::MemControl *mem) const {
        return (entrys[i].pointer.pointer(mem->BaseAddr()));
    }

    PointerBEntry(uint64_t key, int prefix_len, CLevel::MemControl* mem = nullptr) {
        memset(this, 0, sizeof(PointerBEntry));
        entrys[0].buf.prefix_bytes = prefix_len;
        entrys[0].buf.suffix_bytes = 8 - prefix_len;
        entrys[0].buf.entries = 1;
        entrys[0].entry_key = key;
        entrys[0].pointer.Setup(mem, key, prefix_len);
        clflush((void *)&entrys[0]);
    //   clevel.Setup(mem, buf.suffix_bytes);
      // std::cout << "Entry key: " << key << std::endl;
    }

    PointerBEntry(uint64_t key, uint64_t value, int prefix_len, CLevel::MemControl* mem = nullptr)
    {
        memset(this, 0, sizeof(PointerBEntry));
        entrys[0].buf.prefix_bytes = prefix_len;
        entrys[0].buf.suffix_bytes = 8 - prefix_len;
        entrys[0].buf.entries = 1;
        entrys[0].entry_key = key;
        entrys[0].pointer.Setup(mem, key, prefix_len);
        (entrys[0].pointer.pointer(mem->BaseAddr()))->Put(mem, key, value);
        clflush(&entrys[0]);
        // std::cout << "Entry key: " << key << std::endl;
    }

    PointerBEntry(const entry *entry, CLevel::MemControl* mem = nullptr)
    {
        memset(this, 0, sizeof(PointerBEntry));
        entrys[0] = *entry;
        entrys[0].buf.entries = 1;
        clflush(&entrys[0]);
        // std::cout << "Entry key: " << key << std::endl;
    }

    /**
     * @brief 调整第一个entry的entry_key，用于可能存在插入比当前最小key还要小的情况
     * 
     * @param mem 
     */
    void AdjustEntryKey(CLevel::MemControl* mem) {
        // entry_key = Pointer(0, mem)->key(0);
    }

    /**
     * @brief 根据key找到C层节点位置
     * 
     * @param key 
     * @return int 
     */
    int Find_pos(uint64_t key) const {
        int pos = 0;
        while(pos < buf.entries && entrys[pos].IsValid() && entrys[pos].entry_key <= key) pos ++;
        pos = pos == 0 ? pos : pos -1;
        return pos;
    }
    
    status Put(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
        retry:
        // Common::timers["ALevel_times"].start();
        int pos = Find_pos(key);
        if (unlikely(!entrys[pos].IsValid())) {
            entrys[pos].pointer.Setup(mem, key, entrys[pos].buf.prefix_bytes);
        }
        // Common::timers["ALevel_times"].end();
        // std::cout << "Put key: " << key << ", value " << value << std::endl;
        auto ret = (entrys[pos].pointer.pointer(mem->BaseAddr()))->Put(mem, key, value);
        if(ret == status::Full && entrys[0].buf.entries < entry_count) { // 节点满的时候进行扩展
            buncket_t *next = nullptr;
            uint64_t split_key;
            int prefix_len = 0;
            (entrys[pos].pointer.pointer(mem->BaseAddr()))->Expand_(mem, next, split_key, prefix_len);
            for(int i = entrys[0].buf.entries - 1; i > pos; i --) {
                entrys[i + 1] = entrys[i];
            }
            entrys[pos + 1].entry_key = split_key;
            entrys[pos + 1].pointer.Setup(mem, next, key, prefix_len);
            entrys[pos + 1].buf.prefix_bytes = prefix_len;
            entrys[pos + 1].buf.suffix_bytes = 8 - prefix_len;
            entrys[0].buf.entries ++;
            // this->Show(mem);
            clflush(&entrys[0]);
            goto retry;
        }
        // if(ret != status::OK) {
        //     std::cout << "Put failed " << ret << std::endl;
        // }
        if(ret == status::OK && entry_key > key) {
            entry_key = key;
        }
        return ret;
    }
    bool Update(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
        int pos = Find_pos(key);
        if (unlikely(pos >= entry_count || !entrys[pos].IsValid())) {
            return false;
        }
        auto ret = (entrys[pos].pointer.pointer(mem->BaseAddr()))->Update(mem, key, value);
        return ret == status::OK;
    }
    bool Get(CLevel::MemControl* mem, uint64_t key, uint64_t& value) const {
        int pos = Find_pos(key);
        if (unlikely(pos >= entry_count || !entrys[pos].IsValid())) {
            return false;
        }
        auto ret = (entrys[pos].pointer.pointer(mem->BaseAddr()))->Get(mem, key, value);
        return ret == status::OK;
    }

    bool Delete(CLevel::MemControl* mem, uint64_t key, uint64_t* value) {
        int pos = Find_pos(key);
        if (unlikely(pos >= entry_count || !entrys[pos].IsValid())) {
            return false;
        }
        auto ret = (entrys[pos].pointer.pointer(mem->BaseAddr()))->Delete(mem, key, value);
        return ret == status::OK;
    }

    void Show(CLevel::MemControl* mem) {
        for(int i = 0; i < buf.entries && entrys[i].IsValid(); i ++) {
            std::cout << "Entry key: "  << entrys[i].entry_key << std::endl;
            (entrys[i].pointer.pointer(mem->BaseAddr()))->Show();
        }
    }

    // Only use when expand
    status Load(CLevel::MemControl* mem, uint64_t *keys, uint64_t *values, int count) {
        assert(buf.entries == 1);
        Pointer(0, mem)->Load(keys, values, count);
        return status::OK;
    }
    // FIXME: clflush and fence?
    void SetInvalid() { entrys[0].buf.meta = 0; }
    bool IsValid()    { return entrys[0].buf.meta != 0; }

    class Iter {
     public:
      Iter() {}

      Iter(const PointerBEntry* entry, const CLevel::MemControl* mem)
        : entry_(entry), mem_(mem)
      {
          if(entry_->entrys[0].IsValid()) {
              new (&biter_) buncket_t::Iter(entry_->Pointer(0, mem), entry_->entrys[0].buf.prefix_bytes);
          }
          cur_idx = 0;
      }

      Iter(const PointerBEntry* entry, const CLevel::MemControl* mem, uint64_t start_key)
        : entry_(entry), mem_(mem)
      {
          int pos = entry_->Find_pos(start_key);
          if (unlikely(!entry_->entrys[pos].IsValid())) {
              cur_idx = entry_count;
              return ;
          }
          cur_idx = pos;
          if(entry_->entrys[pos].IsValid()) {
              new (&biter_) buncket_t::Iter(entry_->Pointer(pos, mem), entry_->entrys[pos].buf.prefix_bytes, start_key);
              if(biter_.end()) {
                  next();
              }
          }
      }

      ALWAYS_INLINE uint64_t key() const {
        return biter_.key();
      }

      ALWAYS_INLINE uint64_t value() const {
        return biter_.value();
      }

      ALWAYS_INLINE bool next() {
          if(cur_idx <  entry_->buf.entries && biter_.next()) {
              return true;
          } else if(cur_idx < entry_->buf.entries - 1) {
              cur_idx ++;
              new (&biter_) buncket_t::Iter(entry_->Pointer(cur_idx, mem_), entry_->entrys[cur_idx].buf.prefix_bytes);
              if(biter_.end()) return false;
              return true;
          }
          cur_idx = entry_->buf.entries;
          return false;
      }

      ALWAYS_INLINE bool end() const {
        return cur_idx >= entry_->buf.entries;
      }

     private:
      const PointerBEntry* entry_;
      int cur_idx;
      const CLevel::MemControl *mem_;
      buncket_t::Iter biter_;
    };
    using NoSortIter = Iter;


    class EntryIter {
     public:
      EntryIter() {}

      EntryIter(const PointerBEntry* entry)
        : entry_(entry)
      {
          cur_idx = 0;
      }

    //   ALWAYS_INLINE uint64_t key() const {
    //     return biter_.key();
    //   }

    //   ALWAYS_INLINE uint64_t value() const {
    //     return biter_.value();
    //   }

      const PointerBEntry::entry& operator*() { return entry_->entrys[cur_idx]; }


      ALWAYS_INLINE bool next() {
          cur_idx ++;
          if(cur_idx <  entry_->buf.entries) {
              return true;
          } 
          cur_idx = entry_->buf.entries;
          return false;
      }

      ALWAYS_INLINE bool end() const {
        return cur_idx >= entry_->buf.entries;
      }

     private:
      const PointerBEntry* entry_;
      int cur_idx;
    };
};

/**
 * @brief 合并左右节点，并插入KV对
 * 
 * @param left 
 * @param right 
 * @param mem 
 * @param key 
 * @param value 
 * @return status 
 */
static inline status MergePointerBEntry(PointerBEntry *left, PointerBEntry *right,
        CLevel::MemControl* mem, uint64_t key, uint64_t value)
{
    if(left->buf.entries == PointerBEntry::entry_count) {
        // simple move one to left
        int right_entries = right->buf.entries;
        std::copy(&right->entrys[0], &right->entrys[right_entries], &right->entrys[1]);
        // memmove(&right->entrys[1], &right->entrys[0], sizeof(PointerBEntry::entry) * (right->buf.entries));
        right->entrys[0] = left->entrys[left->buf.entries - 1];
        right->buf.entries = right->entrys[1].buf.entries + 1;
        clflush(right);
        fence();
        // NVM::Mem_persist(right, sizeof(PointerBEntry));
        left->entrys[left->buf.entries - 1].SetInvalid();
        left->buf.entries -= 1;
        clflush(left);
        fence();
        // NVM::Mem_persist(left, sizeof(PointerBEntry)); 
    } else {
        left->entrys[left->buf.entries] = right->entrys[0];
        left->buf.entries += 1;
        clflush(left);
        fence();
        // NVM::Mem_persist(left, sizeof(PointerBEntry)); 

        int right_entries = right->buf.entries;
        std::copy(&right->entrys[1], &right->entrys[right_entries], &right->entrys[0]);
        // memmove(&right->entrys[0], &right->entrys[1], sizeof(PointerBEntry::entry) * (right_entries - 1));
        right->entrys[right_entries - 1].SetInvalid();
        right->buf.entries = right_entries - 1;
        clflush(right);
        fence();
        // NVM::Mem_persist(right, sizeof(PointerBEntry));
    }
    if(key < right->entry_key) {
        return left->Put(mem, key, value);
    } 
    else {
        return right->Put(mem, key, value);
    }
}
 // namespace combotree
}

