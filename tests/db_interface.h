#pragma once
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstring>
#include <future>
#include "ycsb/ycsb-c.h"
#include "combotree/combotree.h"
#include "../src/combotree_config.h"
#include "fast-fair/btree.h"
#include "fast-fair/btree_old.h"

#ifdef USE_MEM

#include "mem/pgm/pgm_index_dynamic.hpp"
#include "mem/xindex/xindex_impl.h"
#include "mem/alex/alex.h"
#else
#include "learnindex/pgm_index_dynamic.hpp"
#include "learnindex/rmi.h"
#include "xindex/xindex_impl.h"
#include "alex/alex.h"
#endif

#include "stx/btree_map.h"
#include "../src/letree.h"
#include "random.h"

using combotree::ComboTree;
using FastFair::btree;
using namespace std;

namespace KV {
  class Key_t {
  typedef std::array<double, 1> model_key_t;

 public:
  static constexpr size_t model_key_size() { return 1; }
  static Key_t max() {
    static Key_t max_key(std::numeric_limits<uint64_t>::max());
    return max_key;
  }
  static Key_t min() {
    static Key_t min_key(std::numeric_limits<uint64_t>::min());
    return min_key;
  }

  Key_t() : key(0) {}
  Key_t(uint64_t key) : key(key) {}
  Key_t(const Key_t &other) { key = other.key; }
  Key_t &operator=(const Key_t &other) {
    key = other.key;
    return *this;
  }

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = key;
    return model_key;
  }

  friend bool operator<(const Key_t &l, const Key_t &r) { return l.key < r.key; }
  friend bool operator>(const Key_t &l, const Key_t &r) { return l.key > r.key; }
  friend bool operator>=(const Key_t &l, const Key_t &r) { return l.key >= r.key; }
  friend bool operator<=(const Key_t &l, const Key_t &r) { return l.key <= r.key; }
  friend bool operator==(const Key_t &l, const Key_t &r) { return l.key == r.key; }
  friend bool operator!=(const Key_t &l, const Key_t &r) { return l.key != r.key; }

  uint64_t key;
};
}

namespace dbInter {

static inline std::string human_readable(double size) {
  static const std::string suffix[] = {
    "B",
    "KB",
    "MB",
    "GB"
  };
  const int arr_len = 4;

  std::ostringstream out;
  out.precision(2);
  for (int divs = 0; divs < arr_len; ++divs) {
    if (size >= 1024.0) {
      size /= 1024.0;
    } else {
      out << std::fixed << size;
      return out.str() + suffix[divs];
    }
  }
  out << std::fixed << size;
  return out.str() + suffix[arr_len - 1];
}

class FastFairDb : public ycsbc::KvDB {
public:
    FastFairDb(): tree_(nullptr) {}
    FastFairDb(btree *tree): tree_(tree) {}
    virtual ~FastFairDb() {}
    void Init()
    {
      NVM::data_init(); 
      tree_ = new btree();
    }

    void Info()
    {
      NVM::show_stat();
    }

    void Close() { 

    }
    int Put(uint64_t key, uint64_t value) 
    {
        tree_->btree_insert(key, (char *)value);
        return 1;
    }
    int Get(uint64_t key, uint64_t &value)
    {
        value = (uint64_t)tree_->btree_search(key);
        return 1;
    }
    int Update(uint64_t key, uint64_t value) {
        tree_->btree_delete(key);
        tree_->btree_insert(key, (char *)value);
        return 1;
    }

    int Delete(uint64_t key) {
        tree_->btree_delete(key);
        return 1;
    }

    int Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>>& results) 
    {
        tree_->btree_search_range(start_key, UINT64_MAX, results, len);
        return 1;
    }
    void PrintStatic() {
        NVM::show_stat();
        tree_->PrintInfo();
    }
private:
    btree *tree_;
};

class ComboTreeDb : public ycsbc::KvDB {
public:
    ComboTreeDb(): tree_(nullptr) {}
    ComboTreeDb(ComboTree *tree): tree_(tree) {}
    virtual ~ComboTreeDb() {}

    void Init()
    {
#ifdef SERVER
      tree_ = new ComboTree("/pmem0/", (1024*1024*1024*100UL), true);
#else
      tree_ = new ComboTree("/mnt/pmem0/", (1024*1024*512UL), true);
#endif
    }

    void Info()
    {
      NVM::show_stat();
    }
    int Put(uint64_t key, uint64_t value) 
    {
        tree_->Put(key, value);
        return 1;
    }
    int Get(uint64_t key, uint64_t &value)
    {
        tree_->Get(key, value);
        return 1;
    }
    int Update(uint64_t key, uint64_t value) {
        tree_->Update(key, value);
        return 1;
    }

    int Delete(uint64_t key) {
        tree_->Delete(key);
        return 1;
    }
    int Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>>& results) 
    {
        tree_->Scan(start_key, len, results);
        return 1;
    }

    void PrintStatic() {
      // std::cerr << "Alevel average cost: " << Common::timers["ALevel_times"].avg_latency();
      // std::cerr << ",Blevel average cost: " << Common::timers["BLevel_times"].avg_latency();
      // std::cerr << ",Clevel average cost: " << Common::timers["CLevel_times"].avg_latency() << std::endl;
      // Common::timers["ALevel_times"].clear();
      // Common::timers["BLevel_times"].clear();
      // Common::timers["CLevel_times"].clear();
       // statistic
        // std::cout << "clevel time:    " << tree_->CLevelTime()/1000000.0 << std::endl;
        // std::cout << "entries:        " << tree_->BLevelEntries() << std::endl;
        // std::cout << "clevels:        " << tree_->CLevelCount() << std::endl;
        // std::cout << "clevel percent: " << (double)tree_->CLevelCount() / tree_->BLevelEntries() * 100.0 << "%" << std::endl;
        // std::cout << "size:           " << tree_->Size() << std::endl;
        std::cout << "usage:          " << human_readable(tree_->Usage()) << std::endl;
        // std::cout << "bytes-per-pair: " << (double)tree_->Usage() / tree_->Size() << std::endl;
        // tree_->BLevelCompression();
    }
private:
    ComboTree *tree_;
};

class PGMDynamicDb : public ycsbc::KvDB {
#ifdef USE_MEM
  using PGMType = pgm::PGMIndex<uint64_t>;
#else
  using PGMType = PGM_OLD_NVM::PGMIndex<uint64_t>;
#endif
  typedef pgm::DynamicPGMIndex<uint64_t, char *, PGMType> DynamicPGM;
public:
  PGMDynamicDb(): pgm_(nullptr) {}
  PGMDynamicDb(DynamicPGM *pgm): pgm_(pgm) {}
  virtual ~PGMDynamicDb() {
    delete pgm_;
  }

  void Init()
  {
    NVM::data_init();
    pgm_ = new DynamicPGM();
  }

  void Bulk_load(const std::pair<uint64_t, uint64_t> data[], int size) {
    
    if(pgm_) delete pgm_;
    pgm_ = new DynamicPGM(&data[0], &data[0] + size);
  }

  void Info()
  {
    NVM::show_stat();
  }

  void Begin_trans()
  {
    pgm_->trans_to_read();
  }
  int Put(uint64_t key, uint64_t value) 
  {
    pgm_->insert(key, (char *)value);
    return 1;
  }
  int Get(uint64_t key, uint64_t &value)
  {
      auto it = pgm_->find(key);
      value = (uint64_t)it->second;
      return 1;
  }
  int Update(uint64_t key, uint64_t value) {
      pgm_->insert(key, (char *)value);
      return 1;
  }

  int Delete(uint64_t key) {
      pgm_->erase(key);
      return 1;
  }
  int Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>>& results) 
  {
    int scan_count = 0;
    auto it = pgm_->find(start_key);
    while(it != pgm_->end() && scan_count < len) {
      results.push_back({it->first, (uint64_t)it->second});
      ++it;
      scan_count ++;
    }  
    return 1;
  }
  void PrintStatic() {
    NVM::show_stat();
  }
private:
  DynamicPGM *pgm_;
};

class XIndexDb : public ycsbc::KvDB  {
  static const int init_num = 10000;
  int bg_num_ = 1;
  int work_num_ = 1;
  typedef KV::Key_t index_key_t;
  typedef xindex::XIndex<index_key_t, uint64_t> xindex_t;
public:
  XIndexDb(): xindex_(nullptr) {}
  XIndexDb(int bg_num, int work_num): xindex_(nullptr),
         bg_num_(bg_num), work_num_(work_num) {

  }

  XIndexDb(xindex_t *xindex): xindex_(xindex) {}
  virtual ~XIndexDb() {
    delete xindex_;
  }

  void Init()
  {
    NVM::data_init();
    // prepare_xindex(init_num, work_num, bg_num);
  }

  void Bulk_load(const std::pair<uint64_t, uint64_t> data[], int size) {
    std::vector<index_key_t> initial_keys;
    std::vector<uint64_t> vals;
    initial_keys.resize(size);
    vals.resize(size);
    for (size_t i = 0; i < size; ++i) {
      initial_keys[i] = data[i].first;
      vals[i] = data[i].second;
    }
    if(xindex_) delete xindex_;

    xindex_ = new xindex_t(initial_keys, vals, work_num_, bg_num_);
  }

  void Info()
  {
    NVM::show_stat();
  }

  int Put(uint64_t key, uint64_t value) 
  {
    // pgm_->insert(key, (char *)value);
    xindex_->put(index_key_t(key), value >> 4, 0);
    return 1;
  }
  int Get(uint64_t key, uint64_t &value)
  {
      xindex_->get(index_key_t(key), value, 0);
      return 1;
  }
  int Update(uint64_t key, uint64_t value) {
      xindex_->put(index_key_t(key), value >> 4, 0);
      return 1;
  }
  int Delete(uint64_t key) {
      xindex_->remove(key, 0);
      return 1;
  }

  int MultPut(uint64_t key, uint64_t value, int work_id) 
  {
    xindex_->put(index_key_t(key), value, work_id);
    return 1;
  }
  int MultGet(uint64_t key, uint64_t &value, int work_id) 
  {
    xindex_->get(index_key_t(key), value, work_id);
    return 1;
  }
  int MultDelete(uint64_t key, int work_id) 
  {
    xindex_->remove(key, work_id);;
    return 1;
  }

  int Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>>& results) 
  {
    std::vector<std::pair<index_key_t, uint64_t>> tmpresults;
    xindex_->scan(index_key_t(start_key), len, tmpresults, 0);
    return 1;
  } 
  void PrintStatic() {
        NVM::show_stat();
    }
private:
  inline void 
  prepare_xindex(size_t init_size, int fg_n, int bg_n) {
    // prepare data
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> rand_int64(
        0, std::numeric_limits<int64_t>::max());
    std::vector<index_key_t> initial_keys;
    initial_keys.reserve(init_size);
    for (size_t i = 0; i < init_size; ++i) {
      initial_keys.push_back(index_key_t(rand_int64(gen)));
    }
    // initilize XIndex (sort keys first)
    std::sort(initial_keys.begin(), initial_keys.end());
    std::vector<uint64_t> vals(initial_keys.size(), 1);
    xindex_ = new xindex_t(initial_keys, vals, fg_n, bg_n);
  }
  xindex_t *xindex_;
};

class StxDB : public ycsbc::KvDB  {
  typedef uint64_t KEY_TYPE;
  typedef uint64_t PAYLOAD_TYPE;
  typedef stx::btree_map<KEY_TYPE, PAYLOAD_TYPE> btree_t;
public:
  StxDB(): btree_(nullptr) {}
  StxDB(btree_t *btree): btree_(btree) {}
  virtual ~StxDB() {
    delete btree_;
  }

  void Init()
  {
    btree_ = new btree_t();
  }

  void Info()
  {
  }

  int Put(uint64_t key, uint64_t value) 
  {
    btree_->insert(key, value);
    return 1;
  }
  int Get(uint64_t key, uint64_t &value)
  {
      value = btree_->find(key).data();
      // assert(value == key);
      return 1;
  }
  int Update(uint64_t key, uint64_t value) {
      btree_->find(key).data() = value;
      return 1;
  }
  int Delete(uint64_t key) {
      btree_->erase(key);
      return 1;
  }
  int Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>>& results) 
  {
    auto it = btree_->lower_bound(start_key);
    int num_entries = 0;
    while (num_entries < len && it != btree_->end()) {
      results.push_back({it.key(), it.data()});
      num_entries ++;
      it++;
    }
    return 1;
  } 
  void PrintStatic() {
    // std::cerr << "Alevel average cost: " << Common::timers["ABLevel_times"].avg_latency() << std::endl;
    // std::cerr << "Clevel average cost: " << Common::timers["CLevel_times"].avg_latency() << std::endl;
  }
private:
  btree_t *btree_;
};

class AlexDB : public ycsbc::KvDB  {
  typedef uint64_t KEY_TYPE;
  typedef uint64_t PAYLOAD_TYPE;
#ifdef USE_MEM
  using Alloc = std::allocator<std::pair<KEY_TYPE, PAYLOAD_TYPE>>;
#else
  using Alloc = NVM::allocator<std::pair<KEY_TYPE, PAYLOAD_TYPE>>;
#endif
  typedef alex::Alex<KEY_TYPE, PAYLOAD_TYPE, alex::AlexCompare, Alloc> alex_t;
public:
  AlexDB(): alex_(nullptr) {}
  AlexDB(alex_t *alex): alex_(alex) {}
  virtual ~AlexDB() {
    delete alex_;
  }

  void Init()
  {
    NVM::data_init();
    alex_ = new alex_t();
  }

  void Bulk_load(const std::pair<uint64_t, uint64_t> data[], int size) {
    alex_->bulk_load(data, size);
  }

  void Info()
  {
    NVM::show_stat();
  }

  int Put(uint64_t key, uint64_t value) 
  {
    alex_->insert(key, value);
    return 1;
  }
  int Get(uint64_t key, uint64_t &value)
  {
      value = *(alex_->get_payload(key));
      // assert(value == key);
      return 1;
  }
  int Update(uint64_t key, uint64_t value) {
      uint64_t *addrs = (alex_->get_payload(key));
      *addrs = value;
      NVM::Mem_persist(addrs, sizeof(uint64_t));
      return 1;
  }
  int Delete(uint64_t key) {
      alex_->erase(key);
      return 1;
  }
  int Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>>& results) 
  {
    auto it = alex_->lower_bound(start_key);
    int num_entries = 0;
    while (num_entries < len && it != alex_->end()) {
      results.push_back({it.key(), it.payload()});
      num_entries ++;
      it++;
    }
    return 1;
  } 
  void PrintStatic() {
    // std::cerr << "Alevel average cost: " << Common::timers["ABLevel_times"].avg_latency() << std::endl;
    // std::cerr << "Clevel average cost: " << Common::timers["CLevel_times"].avg_latency() << std::endl;
    NVM::show_stat();  
  }
private:
  alex_t *alex_;
};

class fastfairDB : public ycsbc::KvDB  {
  typedef uint64_t KEY_TYPE;
  typedef uint64_t PAYLOAD_TYPE;
  typedef fastfair::btree btree_t;
public:
  fastfairDB(): tree_(nullptr) {}
  fastfairDB(btree_t *btree): tree_(btree) {}
  virtual ~fastfairDB() {
    delete tree_;
  }

  void Init()
  {
    NVM::data_init();
    tree_ = new btree_t();
  }

  void Info()
  {
    NVM::show_stat();
  }

  void Bulk_load(const std::pair<uint64_t, uint64_t> data[], int size) {
    for(int  i = 0; i < size; i ++) {
      tree_->btree_insert(data[i].first, (char *)data[i].second);
    }
  }

  void Close() { 

    }
    int Put(uint64_t key, uint64_t value) 
    {
        tree_->btree_insert(key, (char *)value);
        return 1;
    }
    int Get(uint64_t key, uint64_t &value)
    {
        value = (uint64_t)tree_->btree_search(key);
        return 1;
    }
    int Update(uint64_t key, uint64_t value) {
        tree_->btree_delete(key);
        tree_->btree_insert(key, (char *)value);
        return 1;
    }

    int Delete(uint64_t key) {
        tree_->btree_delete(key);
        return 1;
    }

    int Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>>& results) 
    {
        // tree_->btree_search_range(start_key, UINT64_MAX, results, len);
        return 1;
    }
    void PrintStatic() {
        NVM::show_stat();
    }
private:
  btree_t *tree_;
};

class LetDB : public ycsbc::KvDB  {
  static const size_t init_num = 2000;
  void Prepare() {
    // std::vector<std::pair<uint64_t,uint64_t>> initial_kv;
    // combotree::Random rnd(0, UINT64_MAX - 1);
    // initial_kv.push_back({0, UINT64_MAX});
    // for (uint64_t i = 0; i < init_num - 1; ++i) {
    //     uint64_t key = rnd.Next();
    //     uint64_t value = rnd.Next();
    //     initial_kv.push_back({key, value});
    // }
    // sort(initial_kv.begin(), initial_kv.end());
    // let_->bulk_load(initial_kv);
  }
public:
  LetDB(): let_(nullptr) {}
  LetDB(combotree::letree *root): let_(root) {}
  virtual ~LetDB() {
    delete let_;
  }

  void Init()
  {
    NVM::data_init();
    let_ = new combotree::letree();
    let_->Init();
  }

  void Info()
  {
    NVM::show_stat();
    let_->Info();
  }

  virtual void Bulk_load(const std::pair<uint64_t, uint64_t> data[], int size) {
    let_->bulk_load(data, size);
  }

  int Put(uint64_t key, uint64_t value) 
  {
    // alex_->insert(key, value);
    let_->Put(key, value);
    return 1;
  }

  int Get(uint64_t key, uint64_t &value)
  {
      let_->Get(key, value);
      return 1;
  }
  int Delete(uint64_t key) {
      let_->Delete(key);
      return 1;
  }
  int Update(uint64_t key, uint64_t value) {
      let_->Update(key, value);
      return 1;
  }

  int MultPut(uint64_t key, uint64_t value, int work_id) { 
    let_->Put(key, value);
    return 1;
  }
  int MultGet(uint64_t key, uint64_t &value, int work_id) { 
    let_->Get(key, value);
    return 1;
  }

  int MultDelete(uint64_t key, int work_id) { 
    let_->Delete(key);
    return 0;
  }

  int Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>>& results) 
  {
    combotree::letree::Iter it(let_, start_key);
    int num_entries = 0;
    while (num_entries < len && !it.end()) {
      results.push_back({it.key(), it.value()});
      num_entries ++;
      it.next();
    }
    return 1;
  } 

  void Begin_trans() {
    // let_->ExpandTree();
  }
  void PrintStatic() {
      Common::g_metic.show_metic();
  }
private:
  combotree::letree *let_;
};

} //namespace dbInter
