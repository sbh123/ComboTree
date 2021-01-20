#pragma once

#include <atomic>
#include <cstdint>
#include <iostream>
#include <cstddef>
#include <vector>
#include <shared_mutex>
#include "combotree/combotree.h"
#include "combotree_config.h"
#include "bitops.h"
#include "nvm_alloc.h"
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

template<const size_t bucket_size = 256, const size_t value_size = 8>
class Buncket { // without Buncket main key
    ALWAYS_INLINE size_t maxEntrys(int idx) const {
        return max_entries;
    }

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
      clflush(pvalue(target_idx));
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
        int expand_pos = entries / 2;
        next = new (mem->Allocate<Buncket>()) Buncket(key(entries / 2), prefix_bytes);
        int idx = 0;
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
        if(!find && this->value(pos) == 0) {
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

    void Show() {
        std::cout << "This: " << this << std::endl;
        for(int i = 0; i < entries; i ++) {
            std::cout << "key: " << key(i) << ", value: " << value(i) << std::endl;
        }
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
typedef Buncket<256, 8> buncket_t;

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
        std::cout << "Entry key: " << key << std::endl;
    }

    int Find_pos(uint64_t key) const {
        int pos = 0;
        while(pos < entry_count && entrys[pos].IsValid() && entrys[pos].entry_key <= key) pos ++;
        pos = pos == 0 ? pos : pos -1;
        return pos;
    }
    
    status Put(CLevel::MemControl* mem, uint64_t key, uint64_t value) {
        retry:
        int pos = Find_pos(key);
        if (unlikely(!entrys[pos].IsValid())) {
            entrys[pos].pointer.Setup(mem, key, entrys[pos].buf.prefix_bytes);
        }
        // std::cout << "Put key: " << key << ", value " << value << std::endl;
        auto ret = (entrys[pos].pointer.pointer(mem->BaseAddr()))->Put(mem, key, value);
        if(ret == status::Full && entrys[0].buf.entries < entry_count) {
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
        if(ret != status::OK) {
            std::cout << "Put failed " << ret << std::endl;
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
        for(int i = 0; i < entry_count && entrys[i].IsValid(); i ++) {
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
};

} // namespace combotree