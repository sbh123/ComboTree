#include <filesystem>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>

namespace combotree {

namespace {

const std::string DEFAULT_PMEMKV_PATH = "pmemkv";
const std::string DEFAULT_COMBO_TREE_PATH = "combo-tree";

} // anonymous namespace

class Manifest {
 public:
  Manifest(std::string dir, size_t size = PMEMOBJ_MIN_POOL, bool create = true)
      : dir_(dir), size_(size)
  {
    if (create) {
      std::string manifest_path = dir_ + "Manifest";
      std::filesystem::remove(manifest_path);
      pop_ = pmem::obj::pool<Root>::create(manifest_path,
          "Combo Tree Manifest", size, 0666);
      root_ = pop_.root();
      pmem::obj::make_persistent_atomic<std::string>(
          pop_, root_->pmemkv_path, dir_ + DEFAULT_PMEMKV_PATH);
      pmem::obj::make_persistent_atomic<std::string>(
          pop_, root_->combo_tree_path, dir_ + DEFAULT_COMBO_TREE_PATH);
      root_->is_combo_tree = 0;
      root_.persist();
    } else {

    }
  }

  const std::string PmemKVPath() const {
    return *root_->pmemkv_path;
  }

  const std::string ComboTreePath() const {
    return *root_->combo_tree_path;
  }

  bool IsComboTree() const {
    return root_->is_combo_tree;
  }

  void SetIsComboTree(int is_combo_tree) {
    pop_.memcpy_persist(&root_->is_combo_tree, &is_combo_tree,
        sizeof(is_combo_tree));
  }

 private:
  std::string dir_;
  size_t size_;

  struct Root {
    pmem::obj::persistent_ptr<std::string> pmemkv_path;
    pmem::obj::persistent_ptr<std::string> combo_tree_path;
    int is_combo_tree;
  };

  pmem::obj::pool<Root> pop_;
  pmem::obj::persistent_ptr<Root> root_;
};

} // namespace combotree