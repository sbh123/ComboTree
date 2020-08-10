#include <filesystem>
#include <thread>
#include <memory>
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
    : pool_dir_(pool_dir), pool_size_(pool_size),
      expand_min_key_(0), expand_max_key_(0), permit_delete_(true)
{
  ValidPoolDir_();
  manifest_ = new Manifest(pool_dir_);
  pmemkv_ = std::make_shared<PmemKV>(manifest_->PmemKVPath());
  status_ = State::USING_PMEMKV;
}

ComboTree::~ComboTree() {
  while (permit_delete_.load() == false) {
    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }
  pmemkv_.reset();
  alevel_.reset();
  blevel_.reset();
}

size_t ComboTree::Size() const {
  if (status_.load() == State::USING_COMBO_TREE ||
      status_.load() == State::COMBO_TREE_EXPANDING) {
    // FIXME: size when expanding?
    return alevel_->Size();
  } else {
    return pmemkv_->Size();
  }
}

void ComboTree::ChangeToComboTree_() {
  State tmp = State::USING_PMEMKV;
  // must change status first
  if (!status_.compare_exchange_strong(tmp, State::PMEMKV_TO_COMBO_TREE)) {
    return;
  }
  permit_delete_.store(false);
  PmemKV::SetWriteUnvalid();
  // wait until no ref to pmemkv
  // get will not ref, so get still work during migration
  // FIXME: have race conditions! maybe one writer thread haven't ref yet.
  while (!pmemkv_->NoWriteRef()) ;
  LOG(Debug::INFO, "start to migrate data from pmemkv to combotree...");
  std::thread change_thread([&](){
    std::filesystem::remove(manifest_->ComboTreePath());
    pop_ = pmem::obj::pool_base::create(manifest_->ComboTreePath(),
        POOL_LAYOUT, pool_size_, 0666);
    Iterator* iter = pmemkv_->begin();
    blevel_ = std::make_shared<BLevel>(pop_, iter, pmemkv_->Size());
    alevel_ = std::make_shared<ALevel>(blevel_);
    // change manifest first
    manifest_->SetIsComboTree(true);
    State tmp = State::PMEMKV_TO_COMBO_TREE;
    // must change status before wating no ref
    bool exchanged = status_.compare_exchange_strong(tmp, State::USING_COMBO_TREE);
    assert(exchanged);
    delete iter;
    PmemKV::SetReadUnvalid();
    while (!pmemkv_->NoReadRef()) ;
    pmemkv_.reset();
    std::filesystem::remove(manifest_->PmemKVPath());
    LOG(Debug::INFO, "finish migrating data from pmemkv to combotree");
    permit_delete_.store(true);
  });
  change_thread.detach();
}

void ComboTree::ExpandComboTree_() {
  LOG(Debug::INFO, "start to expand combotree. current size is %ld", Size());

  // change status
  State tmp = State::USING_COMBO_TREE;
  if (!status_.compare_exchange_strong(tmp, State::COMBO_TREE_EXPANDING)) {
    LOG(Debug::WARNING, "another thread is expanding combotree! exit.");
    return;
  }

  permit_delete_.store(false);
  std::shared_ptr<BLevel> old_blevel = blevel_;
  std::shared_ptr<ALevel> old_alevel = alevel_;
  std::string old_pool_path = manifest_->ComboTreePath();
  std::string new_pool_path = manifest_->NewComboTreePath();

  pmem::obj::pool_base new_pool = pmem::obj::pool_base::create(new_pool_path,
      POOL_LAYOUT, pool_size_, 0666);
  blevel_ = std::make_shared<BLevel>(new_pool, old_blevel);

  std::thread expandion_thread([&,new_pool,old_pool_path,old_alevel,old_blevel]() mutable {
    blevel_->Expansion(old_blevel, expand_min_key_, expand_max_key_);

    std::shared_ptr<ALevel> new_alevel = std::make_shared<ALevel>(blevel_);
    alevel_ = new_alevel;

    expand_min_key_.store(0);
    expand_max_key_.store(0);

    // change status
    State tmp = State::COMBO_TREE_EXPANDING;
    if (!status_.compare_exchange_strong(tmp, State::USING_COMBO_TREE)) {
      LOG(Debug::ERROR,
          "can not change state from COMBO_TREE_EXPANDING to USING_COMBO_TREE!");
    }

    // remove old pool
    pop_.close();
    std::filesystem::remove(old_pool_path);
    pop_ = new_pool;

    // TODO: delete immediately?
    old_alevel.reset();
    old_blevel.reset();

    LOG(Debug::INFO, "finish expanding combotree. current size is %ld", Size());
    permit_delete_.store(true);
  });
  expandion_thread.detach();
}

bool ComboTree::Insert(uint64_t key, uint64_t value) {
  Status s;
  while (true) {
out_while:
    // the order of comparison should not be changed
    if (status_.load() == State::USING_PMEMKV) {
      s = pmemkv_->Insert(key, value);
      if (s == Status::UNVALID)
        continue;
      if (Size() >= PMEMKV_THRESHOLD)
        ChangeToComboTree_();
      break;
    } else if (status_.load() == State::PMEMKV_TO_COMBO_TREE) {
      std::this_thread::sleep_for(std::chrono::microseconds(5));
      continue;
    } else if (status_.load() == State::USING_COMBO_TREE) {
      s = alevel_->Insert(key, value);
      if (s == Status::UNVALID)
        continue;
      if (Size() >= EXPANSION_FACTOR * blevel_->EntrySize())
        ExpandComboTree_();
      break;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      while (true) {
        if (blevel_->Size() >= EXPANSION_FACTOR * blevel_->EntrySize()) {
          std::this_thread::sleep_for(std::chrono::microseconds(5));
          goto out_while;
        } else if (key < expand_min_key_) {
          s = blevel_->Insert(key, value);
          break;
        } else if (key > expand_max_key_) {
          s = alevel_->Insert(key, value);
          if (s == Status::UNVALID) {
            goto out_while;
          }
          break;
        } else {
          std::this_thread::sleep_for(std::chrono::microseconds(5));
          goto out_while;
        }
      }
      break;
    }
  }
  return s == Status::OK;
}

bool ComboTree::Update(uint64_t key, uint64_t value) {
  assert(0);
  return true;
}

bool ComboTree::Get(uint64_t key, uint64_t& value) const {
  Status s;
  while (true) {
    // the order of comparison should not be changed
    if (status_.load() == State::USING_PMEMKV) {
      s = pmemkv_->Get(key, value);
      if (s == Status::UNVALID)
        continue;
      break;
    } else if (status_.load() == State::PMEMKV_TO_COMBO_TREE) {
      s = pmemkv_->Get(key, value);
      if (s == Status::UNVALID)
        continue;
      break;
    } else if (status_.load() == State::USING_COMBO_TREE) {
      s = alevel_->Get(key, value);
      if (s == Status::UNVALID)
        continue;
      break;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      while (true) {
        if (key < expand_min_key_) {
          s = blevel_->Get(key, value);
          break;
        } else if (key > expand_max_key_) {
          s = alevel_->Get(key, value);
          if (s == Status::UNVALID) {
            assert(key < expand_min_key_);
            s = blevel_->Get(key, value);
          }
          break;
        } else {
          std::this_thread::sleep_for(std::chrono::microseconds(5));
        }
      }
      break;
    }
  }
  return s == Status::OK;
}

bool ComboTree::Delete(uint64_t key) {
  Status s;
  while (true) {
    // the order of comparison should not be changed
    if (status_.load() == State::USING_PMEMKV) {
      s = pmemkv_->Delete(key);
      if (s == Status::UNVALID)
        continue;
      break;
    } else if (status_.load() == State::PMEMKV_TO_COMBO_TREE) {
      std::this_thread::sleep_for(std::chrono::microseconds(5));
      continue;
    } else if (status_.load() == State::USING_COMBO_TREE) {
      s = alevel_->Delete(key);
      if (s == Status::UNVALID)
        continue;
      break;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      while (true) {
        if (key < expand_min_key_) {
          s = blevel_->Delete(key);
          break;
        } else if (key > expand_max_key_) {
          s = alevel_->Delete(key);
          if (s == Status::UNVALID) {
            assert(key < expand_min_key_);
            s = blevel_->Delete(key);
          }
          break;
        } else {
          std::this_thread::sleep_for(std::chrono::microseconds(5));
        }
      }
      break;
    }
  }
  return s == Status::OK;
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
      : tree_(tree), iter_(nullptr)
  {
    if (tree_->status_.load() == ComboTree::State::USING_PMEMKV) {
      iter_ = tree_->pmemkv_->begin();
    } else if (tree_->status_.load() == ComboTree::State::USING_COMBO_TREE) {
      iter_ = tree_->blevel_->begin();
    } else if (tree_->status_.load() == ComboTree::State::COMBO_TREE_EXPANDING) {
      iter_ = tree_->blevel_->begin();
    }
  }

  ~Iter() { if (iter_) delete iter_; }

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
    if (tree_->status_.load() == ComboTree::State::COMBO_TREE_EXPANDING) {
      if (target > tree_->expand_max_key_.load()) {
        delete iter_;
        iter_ = tree_->alevel_->begin();
        iter_->Seek(target);
        if (!iter_->Valid()) {
          delete iter_;
          iter_ = tree_->blevel_->begin();
          iter_->Seek(target);
        }
      } else {
        iter_->Seek(target);
      }
    } else {
      iter_->Seek(target);
    }
  }

  void Next() {
    return iter_->Next();
  }

  // TODO: test Prev(), SeekToLast()
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
  // iter->SeekToFirst();
  return iter;
}

Iterator* ComboTree::end() {
  Iterator* iter = new Iter(this);
  // iter->SeekToLast();
  return iter;
}

} // namespace combotree