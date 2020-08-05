#include <filesystem>
#include <thread>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include "combotree/combotree.h"
#include "combotree_config.h"
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
  if (status_.load() == Status::USING_COMBO_TREE ||
      status_.load() == Status::COMBO_TREE_EXPANDING) {
    // FIXME: size when expanding?
    return alevel_->Size();
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
    std::filesystem::remove(manifest_->PmemKVPath());
  });
  change_thread.detach();
  LOG(Debug::INFO, "finish migrating data from pmemkv to combotree");
}

void ComboTree::ExpandComboTree_() {
  LOG(Debug::INFO, "start to expand combotree. current size is %ld", Size());
  Status tmp = Status::USING_COMBO_TREE;
  if (!status_.compare_exchange_strong(tmp, Status::COMBO_TREE_EXPANDING)) {
    LOG(Debug::WARNING, "another thread is expanding combotree! exit.");
    return;
  }

  BLevel* old_blevel = blevel_;
  ALevel* old_alevel = alevel_;
  std::string old_pool_path = manifest_->ComboTreePath();
  std::string new_pool_path = manifest_->NewComboTreePath();

  pmem::obj::pool_base new_pool = pmem::obj::pool_base::create(new_pool_path,
      POOL_LAYOUT, pool_size_, 0666);
  Iterator* iter = old_blevel->begin();
  blevel_ = new BLevel(new_pool, iter, Size());
  delete iter;

  ALevel* new_alevel = new ALevel(blevel_);
  alevel_ = new_alevel;

  // change status
  tmp = Status::COMBO_TREE_EXPANDING;
  if (!status_.compare_exchange_strong(tmp, Status::USING_COMBO_TREE)) {
    LOG(Debug::ERROR,
        "can not change state from COMBO_TREE_EXPANDING to USING_COMBO_TREE!");
  }

  // TODO: delete immediately?
  delete old_alevel;
  delete old_blevel;

  // remove old pool
  pop_.close();
  std::filesystem::remove(old_pool_path);
  pop_ = new_pool;

  LOG(Debug::INFO, "finish expanding combotree. current size is %ld", Size());
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
      if (Size() >= EXPANSION_FACTOR * blevel_->EntrySize()) {
        ExpandComboTree_();
      }
      break;
    } else if (status_.load() == Status::COMBO_TREE_EXPANDING) {
      assert(0);
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
  bool res;
  while (true) {
    // the order of comparison should not be changed
    if (status_.load() == Status::USING_PMEMKV) {
      res = pmemkv_->Delete(key);
      break;
    } else if (status_.load() == Status::PMEMKV_TO_COMBO_TREE) {
      std::this_thread::sleep_for(std::chrono::microseconds(5));
      continue;
    } else if (status_.load() == Status::USING_COMBO_TREE) {
      res = DeleteToComboTree_(key);
      break;
    }
  }
  return res;
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
  bool res = alevel_->Delete(key);
  return res;
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

class ComboTree::Iter : public Iterator {
 public:
  Iter(ComboTree* tree)
      : tree_(tree)
  {
    if (tree_->status_.load() == ComboTree::Status::USING_PMEMKV) {
      iter_ = tree_->pmemkv_->begin();
    } else if (tree_->status_.load() == ComboTree::Status::USING_COMBO_TREE) {
      iter_ = tree_->blevel_->begin();
    }
  }

  bool Valid() const {
    return iter_->Valid();
  }

  bool Begin() const {
    return iter_->Begin();
  }

  bool End() const {
    return iter_->End();
  }

  void SeekToFirst() {
    return iter_->SeekToFirst();
  }

  void SeekToLast() {
    return iter_->SeekToLast();
  }

  void Seek(uint64_t target) {
    return iter_->Seek(target);
  }

  void Next() {
    return iter_->Next();
  }

  void Prev() {
    return iter_->Prev();
  }

  uint64_t key() const {
    return iter_->key();
  }

  uint64_t value() const {
    return iter_->value();
  }

 private:
  ComboTree* tree_;
  Iterator* iter_;
};

Iterator* ComboTree::begin() {
  Iterator* iter = new Iter(this);
  iter->SeekToFirst();
  return iter;
}

Iterator* ComboTree::end() {
  Iterator* iter = new Iter(this);
  iter->SeekToLast();
  return iter;
}

} // namespace combotree