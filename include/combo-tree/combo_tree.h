#pragma once

#include <cstdint>
#include <libpmemobj++/persistent_ptr.hpp>

class ALevel;
class BLevel;
class Manifest;
class PmemKV;

namespace combotree {

class ComboTree {
 public:
  ComboTree(std::string pool_dir, size_t pool_size, bool create = true);

  bool Insert(uint64_t key, uint64_t value);
  bool Update(uint64_t key, uint64_t value);
  bool Get(uint64_t key, uint64_t& value) const;
  bool Delete(uint64_t key);

  size_t Size() const;

  const std::string POOL_LAYOUT = "Combo Tree";

 private:
  // pool_dir_ end with '/'
  const std::string pool_dir_;
  const size_t pool_size_;
  pmem::obj::pool_base pop_;
  ALevel* alevel_;
  BLevel* blevel_;
  PmemKV* pmemkv_;
  Manifest* manifest_;

  void ValidPoolDir_();

  bool UsingComboTree_() const {
    return manifest_->IsComboTree();
  }

  void ChangeToComboTree_();
};

} // namespace combotree