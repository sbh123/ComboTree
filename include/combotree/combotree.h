#pragma once

#include <cstdint>
#include <atomic>
#include <memory>
#include <vector>
#include <libpmemobj++/persistent_ptr.hpp>
#include "combotree/iterator.h"

namespace combotree {

class ALevel;
class BLevel;
class Manifest;
class PmemKV;

struct Pair {
  uint64_t key;
  uint64_t value;
};

class ComboTree {
 public:
  ComboTree(std::string pool_dir, size_t pool_size, bool create = true);
  ~ComboTree();

  bool Insert(uint64_t key, uint64_t value);
  bool Update(uint64_t key, uint64_t value);
  bool Get(uint64_t key, uint64_t& value) const;
  bool Delete(uint64_t key);
  size_t Scan(uint64_t min_key, uint64_t max_key, size_t size,
      std::vector<std::pair<uint64_t, uint64_t>>& results);
  size_t Scan(uint64_t min_key, uint64_t max_key, size_t size,
      Pair* results);

  size_t Size() const;

  Iterator* begin();
  Iterator* end();

 private:
  const std::string POOL_LAYOUT = "Combo Tree";

  class Iter;

  enum class State {
    USING_PMEMKV,
    PMEMKV_TO_COMBO_TREE,
    USING_COMBO_TREE,
    COMBO_TREE_EXPANDING,
  };

  std::string pool_dir_;
  size_t pool_size_;
  pmem::obj::pool_base pop_;
  std::shared_ptr<ALevel> alevel_;
  std::shared_ptr<BLevel> blevel_;
  std::shared_ptr<PmemKV> pmemkv_;
  Manifest* manifest_;
  std::atomic<State> status_;
  // max key finish expanding or in expanding
  std::atomic<uint64_t> expand_min_key_;
  std::atomic<uint64_t> expand_max_key_;
  std::atomic<bool> permit_delete_;

  bool ValidPoolDir_();
  void ChangeToComboTree_();
  void ExpandComboTree_();
  size_t Scan_(uint64_t min_key, uint64_t max_key, size_t size,
      std::function<void(uint64_t,uint64_t)> callback,
      std::function<uint64_t()> cur_max_key);
};

} // namespace combotree