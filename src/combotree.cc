#include <filesystem>
#include <thread>
#include <memory>
#include <iostream>
#include <unistd.h>
#include "combotree/combotree.h"
#include "combotree_config.h"
#include "alevel.h"
#include "blevel.h"
#include "manifest.h"
#include "pmemkv.h"
#include "debug.h"

namespace combotree {

std::mutex log_mutex;

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

size_t ComboTree::CLevelCount() const {
  return blevel_->CountCLevel();
}

size_t ComboTree::BLevelEntries() const {
  return blevel_->Entries();
}

void ComboTree::BLevelCompression() const {
  blevel_->PrefixCompression();
}

uint64_t ComboTree::Usage() const {
  return blevel_->Usage();
}

int64_t ComboTree::CLevelTime() const {
  return blevel_->CLevelTime();
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
  // std::thread change_thread([&](){
    std::vector<std::pair<uint64_t,uint64_t>> exist_kv;
    pmemkv_->Scan(0, UINT64_MAX, UINT64_MAX, exist_kv);

    blevel_ = std::make_shared<BLevel>(exist_kv.size());
    blevel_->Expansion(exist_kv);

    alevel_ = std::make_shared<ALevel>(blevel_);
    // change manifest first
    manifest_->SetIsComboTree(true);
    State s = State::PMEMKV_TO_COMBO_TREE;
    // must change status before wating no ref
    if (!status_.compare_exchange_strong(s, State::USING_COMBO_TREE))
      LOG(Debug::ERROR, "can not change state from PMEMKV_TO_COMBO_TREE to USING_COMBO_TREE!");
    PmemKV::SetReadUnvalid();
    while (!pmemkv_->NoReadRef()) ;
    pmemkv_.reset();
    std::filesystem::remove(manifest_->PmemKVPath());
    LOG(Debug::INFO, "finish migrating data from pmemkv to combotree");
    permit_delete_.store(true);
  // });
  // change_thread.detach();
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

  blevel_ = std::make_shared<BLevel>(old_blevel->Size());

  // std::thread expandion_thread([&,new_pool,old_pool_path,old_alevel,old_blevel]() mutable {
    blevel_->Expansion(old_blevel.get());

    std::shared_ptr<ALevel> new_alevel = std::make_shared<ALevel>(blevel_);
    alevel_ = new_alevel;

    expand_min_key_.store(0);
    expand_max_key_.store(0);

    // change status
    State s = State::COMBO_TREE_EXPANDING;
    if (!status_.compare_exchange_strong(s, State::USING_COMBO_TREE)) {
      LOG(Debug::ERROR,
          "can not change state from COMBO_TREE_EXPANDING to USING_COMBO_TREE!");
    }

    old_alevel.reset();
    old_blevel.reset();

    LOG(Debug::INFO, "finish expanding combotree. current size is %ld, current entry count is %ld", Size(), blevel_->Entries());
    permit_delete_.store(true);
  // });
  // expandion_thread.detach();
}

bool ComboTree::Put(uint64_t key, uint64_t value) {
  int wait = 0;
  int is_expanding = 0;
  int wait_expanding_finish = 0;
  bool ret;
  while (true) {
    // the order of comparison should not be changed
    if (status_.load() == State::USING_PMEMKV) {
      ret = pmemkv_->Put(key, value);
      if (Size() >= PMEMKV_THRESHOLD)
        ChangeToComboTree_();
      break;
    } else if (status_.load() == State::PMEMKV_TO_COMBO_TREE) {
      std::this_thread::sleep_for(std::chrono::microseconds(5));
      wait++;
      continue;
    } else if (status_.load() == State::USING_COMBO_TREE) {
      ret = alevel_->Put(key, value);
      if (Size() >= EXPANSION_FACTOR * BLEVEL_EXPAND_BUF_KEY * blevel_->Entries())
        ExpandComboTree_();
      break;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      assert(0);
      if (blevel_->Size() >= EXPANSION_FACTOR * BLEVEL_EXPAND_BUF_KEY * blevel_->Entries()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        wait++;
        wait_expanding_finish++;
        continue;
      } else if (key < expand_min_key_.load()) {
        assert(0);
        // s = blevel_->Put(key, value) ? Status::OK : Status::ALREADY_EXISTS;
      } else if (key >= expand_max_key_.load()) {
        ret = alevel_->Put(key, value);
      } else {
        assert(0);
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        is_expanding++;
        wait++;
        continue;
      }
      break;
    }
  }
  if (wait >= 50)
    LOG(Debug::WARNING, "wait: %d, wait finish: %d, is expanding: %d", wait, wait_expanding_finish, is_expanding);
  return ret;
}

bool ComboTree::Get(uint64_t key, uint64_t& value) const {
  bool ret;
  while (true) {
    // the order of comparison should not be changed
    if (status_.load() == State::USING_PMEMKV) {
      ret = pmemkv_->Get(key, value);
      break;
    } else if (status_.load() == State::PMEMKV_TO_COMBO_TREE) {
      ret = pmemkv_->Get(key, value);
      break;
    } else if (status_.load() == State::USING_COMBO_TREE) {
      ret = alevel_->Get(key, value);
      break;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      if (key < expand_min_key_.load()) {
        assert(0);
        // ret = blevel_->Get(key, value);
      } else if (key >= expand_max_key_.load()) {
        ret = alevel_->Get(key, value);
      } else {
        assert(0);
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        continue;
      }
      break;
    }
  }
  return ret;
}

bool ComboTree::Delete(uint64_t key) {
  bool ret;
  while (true) {
    // the order of comparison should not be changed
    if (status_.load() == State::USING_PMEMKV) {
      ret = pmemkv_->Delete(key);
      break;
    } else if (status_.load() == State::PMEMKV_TO_COMBO_TREE) {
      std::this_thread::sleep_for(std::chrono::microseconds(5));
      continue;
    } else if (status_.load() == State::USING_COMBO_TREE) {
      ret = alevel_->Delete(key, nullptr);
      break;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      if (key < expand_min_key_.load()) {
        assert(0);
        // s = blevel_->Delete(key) ? Status::OK : Status::DOES_NOT_EXIST;
      } else if (key >= expand_max_key_.load()) {
        ret = alevel_->Delete(key, nullptr);
      } else {
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        continue;
      }
      break;
    }
  }
  return ret;
}

/************************ ComboTree::IterImpl ************************/
class ComboTree::IterImpl {
 public:
  IterImpl(const ComboTree* tree)
    : tree_(tree)
  {
    if (tree_->blevel_ != nullptr) {
      biter_ = new BLevel::Iter(tree_->blevel_.get());
    } else {
      assert(0);
      biter_ = nullptr;
    }
  }

  IterImpl(const ComboTree* tree, uint64_t start_key)
    : tree_(tree)
  {
    if (tree_->blevel_ != nullptr) {
      uint64_t begin, end;
      tree_->alevel_->GetBLevelRange_(start_key, begin, end);
      biter_ = new BLevel::Iter(tree_->blevel_.get(), start_key, begin, end);
    } else {
      assert(0);
      biter_ = nullptr;
    }
  }

  ALWAYS_INLINE uint64_t key() const {
    return biter_->key();
  }

  ALWAYS_INLINE uint64_t value() const {
    return biter_->value();
  }

  ALWAYS_INLINE bool next() {
    return biter_->next();
  }

  ALWAYS_INLINE bool end() const {
    return biter_ == nullptr || biter_->end();
  }

 private:
  const ComboTree* tree_;
  BLevel::Iter* biter_;
};


/************************ ComboTree::Iter ************************/
ComboTree::Iter::Iter(const ComboTree* tree) : pimpl_(new IterImpl(tree)) {}
ComboTree::Iter::Iter(const ComboTree* tree, uint64_t start_key)
  : pimpl_(new IterImpl(tree, start_key)) {}
uint64_t ComboTree::Iter::key() const   { return pimpl_->key(); }
uint64_t ComboTree::Iter::value() const { return pimpl_->value(); }
bool ComboTree::Iter::next()            { return pimpl_->next(); }
bool ComboTree::Iter::end() const       { return pimpl_->end(); }


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