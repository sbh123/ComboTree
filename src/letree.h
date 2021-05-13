#pragma once

#include "combotree_config.h"
#include "kvbuffer.h"
#include "clevel.h"
#include "pmem.h"

#include "bentry.h"
#include "common_time.h"
#include "pointer_bentry.h"
#include "learnindex/learn_index.h"
#include "rmi_model.h"
#include "statistic.h"

namespace combotree
{

typedef combotree::PointerBEntry bentry_t;

/**
 * @brief 根模型，采用的是两层RMI模型，
 * 1. 目前实现需要首先 Load一定的数据作为初始化数据；
 * 2. EXPAND_ALL 宏定义控制采用每次扩展所有EntryGroup，还是采用重复指针一次扩展一个EntryGroup
 */
class letree {
public:
    static const size_t Repeats = 4;
    class Iter;
    class EntryIter;
    letree() : max_entry_count(0), next_entry_count(0) {
        clevel_mem_ = new CLevel::MemControl(CLEVEL_PMEM_FILE, CLEVEL_PMEM_FILE_SIZE);
    }

    letree(CLevel::MemControl *clevel_mem) 
        : clevel_mem_(clevel_mem)
    {

    }
    
    ~letree() {
        if(entry_space)
            NVM::data_alloc->Free(entry_space, max_entry_count * sizeof(bentry_t));
    }

    void bulk_load(std::vector<std::pair<uint64_t,uint64_t>>& data) {
        max_entry_count = data.size();
        bentry_t *new_entry_space = (bentry_t *)NVM::data_alloc->alloc(max_entry_count * sizeof(bentry_t));
        size_t new_entry_count = 0;
        for(size_t i = 0; i < data.size(); i++) {
            new (&new_entry_space[new_entry_count ++]) bentry_t(data[i].first, data[i].second, clevel_mem_);
        }
        NVM::Mem_persist(new_entry_space, max_entry_count * sizeof(bentry_t));
        model.init<bentry_t *, bentry_t>(new_entry_space, new_entry_count, new_entry_count / 100, get_entry_key);
        entry_space = new_entry_space;
        next_entry_count = max_entry_count;
    }


    int find_entry(const uint64_t & key) const {
        int m = model.predict(key);
        m = std::min(std::max(0, m), (int)max_entry_count - 1);

        return exponential_search_upper_bound(m, key);
    }

    inline int exponential_search_upper_bound(int m, const uint64_t & key) const {
        int bound = 1;
        int l, r;  // will do binary search in range [l, r)
        if(entry_space[m].entry_key > key) {
            int size = m;
            while (bound < size && (entry_space[m - bound].entry_key > key)) {
                bound *= 2;
            }
            l = m - std::min<int>(bound, size);
            r = m - bound / 2;
        } else {
            int size = max_entry_count - m;
            while (bound < size && (entry_space[m + bound].entry_key <= key)) {
                bound *= 2;
            }
            l = m + bound / 2;
            r = m + std::min<int>(bound, size);
        }
        return std::max(binary_search_upper_bound(l, r, key) - 1, 0);
    }

    inline int binary_search_upper_bound(int l, int r, const uint64_t& key) const {
        while (l < r) {
            int mid = l + (r - l) / 2;
            if (entry_space[mid].entry_key <= key) {
                l = mid + 1;
            } else {
                r = mid;
            }
        }
        return l;
    }

    inline int binary_search_lower_bound(int l, int r, const uint64_t& key) const {
        while (l < r) {
            int mid = l + (r - l) / 2;
            if (entry_space[mid].entry_key < key) {
                l = mid + 1;
            } else {
                r = mid;
            }
        }
        return l;
    }
    /**
     * @brief 插入KV对，
     * 
     * @param key 
     * @param value 
     * @return status 
     */
    status Put(uint64_t key, uint64_t value) {
    retry0:
        int entry_id = find_entry(key);
        bool split = false;

        auto ret = entry_space[entry_id].Put(clevel_mem_, key, value, &split);

        if(split) {
            next_entry_count ++;
        }

        if(ret == status::Full ) { // LearnGroup数组满了，需要扩展
            expand_tree();
            split = false;
            goto retry0;
        }
        return ret;
    }

    bool Get(uint64_t key, uint64_t& value) const {
        // Common::g_metic.tracepoint("None");
        int entry_id = find_entry(key);
        // Common::g_metic.tracepoint("FindGoup");
        return entry_space[entry_id].Get(clevel_mem_, key, value);
    }

    bool Update(uint64_t key, uint64_t value) {
        int entry_id = find_entry(key);
        auto ret = entry_space[entry_id].Update(clevel_mem_, key, value);
        return ret;
    }

    bool Delete(uint64_t key) {
        int entry_id = find_entry(key);
        auto ret = entry_space[entry_id].Delete(clevel_mem_, key, nullptr);
        return ret;
    }

    static inline uint64_t get_entry_key(const bentry_t &entry) {
        return entry.entry_key;
    }

    void expand_tree() {
        bentry_t::EntryIter it;
        Timer timer;
        timer.Start();
        bentry_t *new_entry_space = (bentry_t *)NVM::data_alloc->alloc(next_entry_count * sizeof(bentry_t));
        size_t new_entry_count = 0;
        entry_space[0].AdjustEntryKey(clevel_mem_);
        for(size_t i = 0; i < max_entry_count; i++) {
            new (&it) bentry_t::EntryIter(&entry_space[i]);
            while(!it.end()) {
                new (&new_entry_space[new_entry_count ++]) bentry_t(&(*it));
                it.next();
            }
        }

        assert(next_entry_count == new_entry_count);

        NVM::Mem_persist(new_entry_space, new_entry_count * sizeof(bentry_t));

        model.init<bentry_t *, bentry_t>(new_entry_space, new_entry_count, new_entry_count / 128, get_entry_key);
        entry_space = new_entry_space;
        max_entry_count = new_entry_count;
        next_entry_count = max_entry_count;


        uint64_t expand_time = timer.End();
        LOG(Debug::INFO, "Finish expanding letree, new entry count %ld,  expansion time is %lfs", 
                max_entry_count, (double)expand_time/1000000.0);
    }

    void Info() {
        std::cout << "nr_entrys: " << max_entry_count << "\t";
        std::cout << "entry size:" << sizeof(bentry_t) << "\t";
        clevel_mem_->Usage();
    }

private:
    size_t max_entry_count;
    size_t next_entry_count;

    // RMI::LinearModel<RMI::Key_64> model;
    // RMI::TwoStageRMI<RMI::Key_64, 3, 2> model;
    LearnModel::rmi_model<uint64_t> model;

    bentry_t *entry_space;
    CLevel::MemControl *clevel_mem_;
};

class letree::EntryIter
{
public:
    using difference_type = ssize_t;
    using value_type = const uint64_t;
    using pointer = const uint64_t *;
    using reference = const uint64_t &;
    using iterator_category = std::random_access_iterator_tag;

    EntryIter(letree *root) : root_(root) { }
    EntryIter(letree *root, uint64_t idx) : root_(root), idx_(idx) { }
    ~EntryIter() {

    }
    uint64_t operator*()
    { return root_->entry_space[idx_].entry_key; }

    EntryIter& operator++()
    { idx_ ++; return *this; }

    EntryIter operator++(int)         
    { return EntryIter(root_, idx_ ++); }

    EntryIter& operator-- ()  
    { idx_ --; return *this; }

    EntryIter operator--(int)         
    { return EntryIter(root_, idx_ --); }

    uint64_t operator[](size_t i) const
    {
        if((i + idx_) > root_->max_entry_count)
        {
            std::cout << "索引超过最大值" << std::endl; 
            // 返回第一个元素
            return root_->entry_space[root_->max_entry_count - 1].entry_key;
        }
        return root_->entry_space[i + idx_].entry_key;
    }
    
    bool operator<(const EntryIter& iter) const { return  idx_ < iter.idx_; }
    bool operator==(const EntryIter& iter) const { return idx_ == iter.idx_ && root_ == iter.root_; }
    bool operator!=(const EntryIter& iter) const { return idx_ != iter.idx_ || root_ != iter.root_; }
    bool operator>(const EntryIter& iter) const { return  idx_ < iter.idx_; }
    bool operator<=(const EntryIter& iter) const { return *this < iter || *this == iter; }
    bool operator>=(const EntryIter& iter) const { return *this > iter || *this == iter; }
    size_t operator-(const EntryIter& iter) const { return idx_ - iter.idx_; }

    const EntryIter& base() { return *this; }

private:
    letree *root_;
    uint64_t idx_;
};

class letree::Iter
{
public:

    Iter(letree *root) : root_(root), biter_(&root_->entry_space[0], root_->clevel_mem_), idx_(0) { }
    Iter(letree *root, uint64_t start_key) : root_(root) {
        idx_ = root->find_entry(start_key);
        new (&biter_) bentry_t::Iter(&root_->entry_space[idx_], root_->clevel_mem_, start_key);
        if(biter_.end()) {
            next();
        }
    }
    ~Iter() {
        
    }
    
    uint64_t key() {
        return biter_.key();
    }

    uint64_t value() {
        return biter_.value();
    }

    bool next() {
        if(idx_ < root_->max_entry_count && biter_.next()) {
            return true;
        }
        // idx_ = root_->NextGroupId(idx_);
        if(idx_ < root_->max_entry_count) {
            new (&biter_) bentry_t::Iter(&root_->entry_space[idx_], root_->clevel_mem_);
            return true;
        }
        return false;
    }

    bool end() {
        return idx_ >= root_->max_entry_count;
    }

private:
    letree *root_;
    bentry_t::Iter biter_;
    uint64_t idx_;
};
   
} // namespace combotree
