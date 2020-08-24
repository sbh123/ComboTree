#include <iostream>
#include <filesystem>
#include "kvbench/kvbench.h"
#include "combotree/combotree.h"

using namespace combotree;

#define COMBO_TREE_DIR  "/mnt/pmem0/combotree/"

template<typename Key, typename Value>
class ComboTreeV2;

template<>
class ComboTreeV2<uint64_t, uint64_t> : public kvbench::DB<uint64_t, uint64_t> {
 public:
  ComboTreeV2() {
    std::filesystem::remove_all(COMBO_TREE_DIR);
    std::filesystem::create_directory(COMBO_TREE_DIR);
    db_ = new ComboTree(COMBO_TREE_DIR, PMEMOBJ_MIN_POOL * 128);
  }

  ~ComboTreeV2() {
    delete db_;
  }

  int Get(uint64_t key, uint64_t* value) {
    uint64_t val;
    db_->Get(key, val);
    return 0;
  }

  int Put(uint64_t key, uint64_t value) {
    db_->Insert(key, key);
    return 0;
  }

  int Update(uint64_t key,  uint64_t value) {
    return 0;
  }

  int Delete(uint64_t key) {
    db_->Delete(key);
    return 0;
  }

  int Scan(uint64_t min_key, std::vector<uint64_t>* values) {
    // Pair results[scan_size_];
    uint64_t results[scan_size_];
    size_t cnt = db_->Scan(min_key, UINT64_MAX, scan_size_, results);
    return cnt;
  }

  std::string Name() const {
    return "Combo Tree V2";
  }

 private:
  ComboTree* db_;
  const int scan_size_ = 100;
};

int main(int argc, char** argv) {
  kvbench::Bench<uint64_t, uint64_t>* bench = new kvbench::Bench<uint64_t, uint64_t>(argc, argv);
  kvbench::DB<uint64_t, uint64_t>* db = new ComboTreeV2<uint64_t, uint64_t>();
  bench->SetDB(db);
  bench->Run();
  delete bench;
  return 0;
}