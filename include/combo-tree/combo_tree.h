#pragma once

#include <cstdint>
#include <atomic>
#include <libpmemobj++/persistent_ptr.hpp>

namespace combotree {

class ALevel;
class BLevel;
class Manifest;
class PmemKV;

class ComboTree {
 public:
  ComboTree(std::string pool_dir, size_t pool_size, bool create = true);

  bool Insert(uint64_t key, uint64_t value);
  bool Update(uint64_t key, uint64_t value);
  bool Get(uint64_t key, uint64_t& value) const;
  bool Delete(uint64_t key);

  size_t Size() const;

  const size_t PMEMKV_THRESHOLD = 1024;

 private:
  const std::string POOL_LAYOUT = "Combo Tree";

  enum class Status {
    USING_PMEMKV,
    PMEMKV_TO_COMBO_TREE,
    USING_COMBO_TREE,
  };

  // pool_dir_ end with '/'
  std::string pool_dir_;
  size_t pool_size_;
  pmem::obj::pool_base pop_;
  ALevel* alevel_;
  BLevel* blevel_;
  PmemKV* pmemkv_;
  Manifest* manifest_;
  std::atomic<Status> status_;

  bool ValidPoolDir_();

  void ChangeToComboTree_();

  bool InsertToComboTree_(uint64_t key, uint64_t value);
  bool UpdateToComboTree_(uint64_t key, uint64_t value);
  bool GetFromComboTree_(uint64_t key, uint64_t& value) const;
  bool DeleteToComboTree_(uint64_t key);
};

} // namespace combotree