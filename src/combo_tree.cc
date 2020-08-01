#include <filesystem>
#include <thread>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include "combo-tree/combo_tree.h"
#include "iterator.h"
#include "alevel.h"
#include "blevel.h"
#include "manifest.h"
#include "pmemkv.h"
#include "debug.h"

namespace combotree {

ComboTree::ComboTree(std::string pool_dir, size_t pool_size, bool create)
    : pool_dir_(pool_dir), pool_size_(pool_size)
{
  ValidPoolDir_();
  manifest_ = new Manifest(pool_dir_);
  pmemkv_ = new PmemKV(manifest_->PmemKVPath());
  status_ = Status::USING_PMEMKV;
}

size_t ComboTree::Size() const {
  if (status_.load() == Status::USING_COMBO_TREE) {
    return blevel_->Size();
  } else {
    return pmemkv_->Size();
  }
}

void ComboTree::ChangeToComboTree_() {
  LOG(Debug::INFO, "start to migrate data from pmemkv to combotree...");
  std::thread change_thread([&](){
    std::filesystem::remove(manifest_->ComboTreePath());
    pop_ = pmem::obj::pool_base::create(manifest_->ComboTreePath(),
        POOL_LAYOUT, pool_size_, 0666);
    Iterator* iter = pmemkv_->begin();
    blevel_ = new BLevel(pop_, iter, pmemkv_->Size());
    alevel_ = new ALevel(blevel_);
    // change manifest first
    manifest_->SetIsComboTree(true);
    Status tmp = Status::PMEMKV_TO_COMBO_TREE;
    // must change status before wating no ref
    bool exchanged = status_.compare_exchange_strong(tmp, Status::USING_COMBO_TREE);
    assert(exchanged);
    delete iter;
    while (!pmemkv_->NoReadRef()) ;
    delete pmemkv_;
  });
  change_thread.detach();
  LOG(Debug::INFO, "finish migrating data from pmemkv to combotree");
}

bool ComboTree::Insert(uint64_t key, uint64_t value) {
  bool res;
  while (true) {
    // the order of comparison should not be changed
    if (status_.load() == Status::USING_PMEMKV) {
      res = pmemkv_->Insert(key, value);
      if (Size() >= PMEMKV_THRESHOLD) {
        Status tmp = Status::USING_PMEMKV;
        // must change status first
        if (status_.compare_exchange_strong(tmp, Status::PMEMKV_TO_COMBO_TREE)) {
          // wait until no ref to pmemkv
          // get will not ref, so get still work during migration
          // FIXME: have race conditions! maybe one writer thread haven't ref yet.
          while (!pmemkv_->NoWriteRef()) ;
          ChangeToComboTree_();
        }
      }
      break;
    } else if (status_.load() == Status::PMEMKV_TO_COMBO_TREE) {
      std::this_thread::sleep_for(std::chrono::microseconds(5));
      continue;
    } else if (status_.load() == Status::USING_COMBO_TREE) {
      res = InsertToComboTree_(key, value);
      break;
    }
  }
  return res;
}

bool ComboTree::Update(uint64_t key, uint64_t value) {
  return true;
}

bool ComboTree::Get(uint64_t key, uint64_t& value) const {
  bool res;
  while (true) {
    // the order of comparison should not be changed
    if (status_.load() == Status::USING_PMEMKV) {
      res = pmemkv_->Get(key, value);
      break;
    } else if (status_.load() == Status::PMEMKV_TO_COMBO_TREE) {
      res = pmemkv_->Get(key, value);
      break;
    } else if (status_.load() == Status::USING_COMBO_TREE) {
      res = GetFromComboTree_(key, value);
      break;
    }
  }
  return res;
}

bool ComboTree::Delete(uint64_t key) {
  return true;
}

bool ComboTree::InsertToComboTree_(uint64_t key, uint64_t value) {
  bool res = alevel_->Insert(key, value);
  return res;
}

bool ComboTree::UpdateToComboTree_(uint64_t key, uint64_t value) {
  return true;
}

bool ComboTree::GetFromComboTree_(uint64_t key, uint64_t& value) const {
  bool res = alevel_->Get(key, value);
  return res;
}

bool ComboTree::DeleteToComboTree_(uint64_t key) {
  return true;
}

namespace {

// https://stackoverflow.com/a/18101042/7640227
bool dir_exists(const std::string& name) {
  return std::filesystem::exists(name) &&
         std::filesystem::is_directory(name);
}

} // anonymous namespace

bool ComboTree::ValidPoolDir_() {
  if (pool_dir_.empty())
    return false;

  if (!dir_exists(pool_dir_))
    return false;

  if (pool_dir_.back() != '/')
    pool_dir_.push_back('/');

  return true;
}

} // namespace combotree