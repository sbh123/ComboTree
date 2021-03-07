#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <future>
#include "ycsb/ycsb-c.h"
#include "nvm_alloc.h"
#include "common_time.h"
#include "../src/combotree_config.h"
#include "combotree/combotree.h"
#include "fast-fair/btree.h"
#include "learnindex/pgm_index_dynamic.hpp"
#include "learnindex/rmi.h"
#include "xindex/xindex_impl.h"
#include "alex/alex.h"
#include "stx/btree_map.h"
#include "../src/learn_group.h"
#include "random.h"

using combotree::ComboTree;
using FastFair::btree;
using namespace std;
using xindex::XIndex;


const char *workloads[] = {
  "workloada.spec",
  "workloadb.spec",
  "workloadc.spec",
  "workloadd.spec",
  "workloade.spec",
  "workloadf.spec",
  // "workloada_insert_0.spec",
  // "workloada_insert_10.spec",
  // "workloada_insert_20.spec",
  // "workloada_insert_50.spec",
  // "workloada_insert_80.spec",
  // "workloada_insert_100.spec",
  // "workload_read.spec",
  // "workload_insert.spec",

};

#define ArrayLen(arry) (sizeof(arry) / sizeof(arry[0]))

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
private:
  DynamicPGM *pgm_;
};

class XIndexDb : public ycsbc::KvDB  {
  static const int init_num = 10000;
  static const int bg_num = 1;
  static const int work_num = 1;
  typedef RMI::Key_64 index_key_t;
  typedef xindex::XIndex<index_key_t, uint64_t> xindex_t;
public:
  XIndexDb(): xindex_(nullptr) {}
  XIndexDb(xindex_t *xindex): xindex_(xindex) {}
  virtual ~XIndexDb() {
    delete xindex_;
  }

  void Init()
  {
    NVM::data_init();
    prepare_xindex(init_num, bg_num, work_num);
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
      xindex_->put(index_key_t(key), value >> 6, 0);
      return 1;
  }
  int Delete(uint64_t key) {
      xindex_->remove(key, 0);
      return 1;
  }
  int Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>>& results) 
  {
    std::vector<std::pair<index_key_t, uint64_t>> tmpresults;
    xindex_->scan(index_key_t(start_key), len, tmpresults, 0);
    return 1;
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
      // uint64_t *addrs = (alex_->get_payload(key));
      // *addrs = value;
      // NVM::Mem_persist(addrs, sizeof(uint64_t));
      alex_->insert(key, value);
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
  }
private:
  alex_t *alex_;
};

class LearnGroupDB : public ycsbc::KvDB  {
  static const size_t init_num = 1000;
  void Prepare() {
    std::vector<std::pair<uint64_t,uint64_t>> initial_kv;
    combotree::Random rnd(0, UINT64_MAX - 1);
    initial_kv.push_back({0, UINT64_MAX});
    for (uint64_t i = 0; i < init_num - 1; ++i) {
        uint64_t key = rnd.Next();
        uint64_t value = rnd.Next();
        initial_kv.push_back({key, value});
    }
    sort(initial_kv.begin(), initial_kv.end());
    root_->Load(initial_kv);
  }
public:
  LearnGroupDB(): root_(nullptr) {}
  LearnGroupDB(combotree::RootModel *root): root_(root) {}
  virtual ~LearnGroupDB() {
    delete root_;
  }

  void Init()
  {
    NVM::data_init();
    root_ = new combotree::RootModel();
    Prepare();
  }

  void Info()
  {
    NVM::show_stat();
    root_->Info();
  }

  int Put(uint64_t key, uint64_t value) 
  {
    // alex_->insert(key, value);
    root_->Put(key, value);
    return 1;
  }
  int Get(uint64_t key, uint64_t &value)
  {
      root_->Get(key, value);
      return 1;
  }
  int Delete(uint64_t key) {
      root_->Delete(key);
      return 1;
  }
  int Update(uint64_t key, uint64_t value) {
      root_->Update(key, value);
      return 1;
  }
  int Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>>& results) 
  {
    combotree::RootModel::Iter it(root_, start_key);
    int num_entries = 0;
    while (num_entries < len && !it.end()) {
      results.push_back({it.key(), it.value()});
      num_entries ++;
      it.next();
    }
    return 1;
  } 
  void PrintStatic() {
      // std::cerr << "Alevel average cost: " << Common::timers["ALevel_times"].avg_latency();
      // std::cerr << ",Blevel average cost: " << Common::timers["BLevel_times"].avg_latency();
      // std::cerr << ",Clevel average cost: " << Common::timers["CLevel_times"].avg_latency() << std::endl;
      // Common::timers["ALevel_times"].clear();
      // Common::timers["BLevel_times"].clear();
      // Common::timers["CLevel_times"].clear();
  }
private:
  combotree::RootModel *root_;
};


void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
int LoadWorkLoad(utils::Properties &props, string &workload);

int YCSB_Run(ycsbc::KvDB *db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading) {
  ycsbc::KvClient<ycsbc::KvDB> client(db, *wl);
  utils::ChronoTimer timer;
  int oks = 0;
  if(is_loading) {
    timer.Start();
  }
  for (int i = 0; i < num_ops; ++i) {
    if (is_loading) {
      oks += client.DoInsert();
    } else {
      oks += client.DoTransaction();
    }
    if(i%10000 == 0) {
      // std::cerr << "Trans: " << i << "\r";
      if(is_loading) {
        auto duration = timer.End<std::chrono::nanoseconds>();
        std::cout << "op " << i << " :average load latency: " << 1.0 * duration / 10000 << " ns." <<std::endl;
      }
      // std::cout << "average load latency: " << duration << std::endl;
      timer.Start();
      db->PrintStatic();
    }
  }
  if(is_loading) {
    auto duration = timer.End();
    std::cout << "op " << num_ops <<  " :average load latency: " << 1.0 * duration / 10000<< std::endl;
  }
  std::cerr << "Trans: " << num_ops <<std::endl;
  return oks;
}

int main(int argc, const char *argv[])
{
    NVM::env_init();
    ycsbc::KvDB *db = nullptr;
    utils::Properties props;
    ycsbc::CoreWorkload wl;
    string workdloads_dir = ParseCommandLine(argc, argv, props);
    string dbName = props["dbname"];
    const int num_threads = 1;//stoi(props.GetProperty("threadcount", "1"));
    // Loads data
    vector<future<int>> actual_ops;
    // Loads data
    int total_ops = 0;
    int sum = 0;

    std::cout << "YCSB test:" << dbName << std::endl;
    if(dbName == "fastfair") {
      db = new FastFairDb();
    } else if(dbName == "pgm") {
      db = new PGMDynamicDb();
    } else if(dbName == "xindex") {
      db = new XIndexDb();
    } else if(dbName == "alex") {
      db = new AlexDB();
    } else if(dbName == "learngroup") {
      db = new LearnGroupDB();
    } else {
      db = new ComboTreeDb();
    }
    db->Init();

    {
      string workload = workdloads_dir + "/" + workloads[0];
      LoadWorkLoad(props, workload);
      wl.Init(props);
      // Loads data
      total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
      utils::Timer<double> timer;
      timer.Start();
      for (int i = 0; i < 1; ++i) {
          actual_ops.emplace_back(async(launch::async,
              YCSB_Run, db, &wl, total_ops / num_threads, true));
      }
      assert((int)actual_ops.size() == num_threads);

      for (auto &n : actual_ops) {
          assert(n.valid());
          sum += n.get();
      }
      double duration = timer.End();
      cout << "# Loading_records:\t" << sum << " throughput (KTPS)" <<endl;
      cout << props["dbname"] << "\tLoad_thread:" << '\t' << 1 << '\t';
      cout << total_ops / duration / 1000 << endl << endl;
    }
    db->Info();
    for(size_t i = 0; i < ArrayLen(workloads); i ++) {
      // cout << "Loads[" << i << "]: " << workloads[i] << endl;
      string workload = workdloads_dir + "/" + workloads[i];
      LoadWorkLoad(props, workload);
      wl.Reload(props);
      // Peforms transactions
      actual_ops.clear();
      total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
      cerr << props["dbname"] << " start \t" << workloads[i] << "\t: ops " << total_ops << endl;
      utils::Timer<double> timer;
      db->Begin_trans();
      timer.Start();
      for (int i = 0; i < num_threads; ++i) {
          actual_ops.emplace_back(async(launch::async,
              YCSB_Run, db, &wl, total_ops / num_threads, false));
      }
      assert((int)actual_ops.size() == num_threads);

      sum = 0;
      for (auto &n : actual_ops) {
          assert(n.valid());
          sum += n.get();
      }
      double duration = timer.End();
      cout << "# Transaction throughput (KTPS)" << endl;
      cout << props["dbname"] << '\t' << workloads[i] << '\t' << num_threads << '\t';
      cout << total_ops / duration / 1000 << endl << endl;
      db->Info();
    }
    delete db;
    NVM::env_exit();
    return 0;
}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      // ifstream input(argv[argindex]);
      // try {
      //   props.Load(input);
      // } catch (const string &message) {
      //   cout << message << endl;
      //   exit(0);
      // }
      // input.close();
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

int LoadWorkLoad(utils::Properties &props, string &workload) {
  ifstream input(workload);
  try {
    props.Load(input);
  } catch (const string &message) {
    cout << message << endl;
    exit(0);
  }
  input.close();
  return 0;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -P propertyfile directory: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}