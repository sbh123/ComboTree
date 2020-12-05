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
int64_t expand_time = 0;

ComboTree::ComboTree(std::string pool_dir, size_t pool_size, bool create)
    : pool_dir_(pool_dir), pool_size_(pool_size), alevel_(nullptr),
      blevel_(nullptr), old_blevel_(nullptr), pmemkv_(nullptr), permit_delete_(true)
{
  ValidPoolDir_();
  manifest_ = new Manifest(pool_dir_);
  pmemkv_ = new PmemKV(manifest_->PmemKVPath());
  status_ = State::USING_PMEMKV;
}

ComboTree::~ComboTree() {
  while (permit_delete_.load() == false) {
    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }
  if (pmemkv_) delete pmemkv_;
  if (alevel_) delete alevel_;
  if (blevel_) delete blevel_;
  if (old_blevel_ && old_blevel_ != blevel_) delete old_blevel_;
}

size_t ComboTree::Size() const {
  if (status_.load() == State::USING_PMEMKV ||
      status_.load() == State::PMEMKV_TO_COMBO_TREE) {
    return pmemkv_->Size();
  } else {
    // FIXME: size when expanding?
    return alevel_->Size();
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
  if (!status_.compare_exchange_strong(tmp, State::PMEMKV_TO_COMBO_TREE))
    return;

  permit_delete_.store(false);
  PmemKV::SetWriteUnvalid();
  // wait until no ref to pmemkv
  // get will not ref, so get still work during migration
  // FIXME: have race conditions! maybe one writer thread haven't ref yet.
  while (!pmemkv_->NoWriteRef()) ;
  LOG(Debug::INFO, "start to migrate data from pmemkv to combotree...");

  std::vector<std::pair<uint64_t,uint64_t>> exist_kv;
  pmemkv_->Scan(0, UINT64_MAX, UINT64_MAX, exist_kv);

  blevel_ = new BLevel(exist_kv.size());
  old_blevel_ = blevel_;
  blevel_->Expansion(exist_kv);

  alevel_ = new ALevel(blevel_);
  // change manifest first
  manifest_->SetIsComboTree(true);
  State s = State::PMEMKV_TO_COMBO_TREE;
  // must change status before wating no ref
  if (!status_.compare_exchange_strong(s, State::USING_COMBO_TREE))
    LOG(Debug::ERROR, "can not change state from PMEMKV_TO_COMBO_TREE to USING_COMBO_TREE!");

  PmemKV::SetReadUnvalid();
  while (!pmemkv_->NoReadRef()) ;
  delete pmemkv_;
  pmemkv_ = nullptr;
  std::filesystem::remove(manifest_->PmemKVPath());
  LOG(Debug::INFO, "finish migrating data from pmemkv to combotree");
  permit_delete_.store(true);
}

void ComboTree::ExpandComboTree_() {
#ifdef BRANGE
  State s = State::USING_COMBO_TREE;
  if (!status_.compare_exchange_strong(s, State::PREPARE_EXPANDING))
    return;

  LOG(Debug::INFO, "preparing to expand combotree. current size is %ld", Size());

  permit_delete_.store(false);
  sleeped_threads_.store(1);
  need_sleep_.store(true);

  Timer timer;
  timer.Start();

  // old_blevel_ is set when last expanding finish.
  blevel_ = new BLevel(old_blevel_->Size());
  blevel_->PrepareExpansion(old_blevel_);

  s = State::PREPARE_EXPANDING;
  if (!status_.compare_exchange_strong(s, State::COMBO_TREE_EXPANDING))
    assert(0);

  blevel_->Expansion(old_blevel_);

  need_sleep_.store(false);

  ALevel* old_alevel_ = alevel_;
  ALevel* new_alevel = new ALevel(blevel_);
  alevel_ = new_alevel;
  delete old_alevel_;

  s = State::COMBO_TREE_EXPANDING;
  if (!status_.compare_exchange_strong(s, State::USING_COMBO_TREE))
    assert(0);

  expand_time += timer.End();
  permit_delete_.store(true);
  delete old_blevel_;
  old_blevel_ = blevel_;

  LOG(Debug::INFO, "finish expanding combotree. current size is %ld, current entry count is %ld, expansion time is %lfs", Size(), blevel_->Entries(), (double)expand_time/1000000.0);

#else // BRANGE

  State s = State::USING_COMBO_TREE;
  if (!status_.compare_exchange_strong(s, State::COMBO_TREE_EXPANDING))
    return;

  permit_delete_.store(false);

  Timer timer;
  timer.Start();

  ALevel* old_alevel = alevel_;
  BLevel* old_blevel = blevel_;

  blevel_ = new BLevel(old_blevel->Size());
  blevel_->Expansion(old_blevel);
  alevel_ = new ALevel(blevel_);

  delete old_alevel;
  delete old_blevel;

  // change status
  s = State::COMBO_TREE_EXPANDING;
  if (!status_.compare_exchange_strong(s, State::USING_COMBO_TREE)) {
    LOG(Debug::ERROR,
        "can not change state from COMBO_TREE_EXPANDING to USING_COMBO_TREE!");
  }

  expand_time += timer.End();
  permit_delete_.store(true);

  LOG(Debug::INFO, "finish expanding combotree. current size is %ld, current entry count is %ld, expansion time is %lfs", Size(), blevel_->Entries(), (double)expand_time/1000000.0);
#endif // BRANGE
}

bool ComboTree::Put(uint64_t key, uint64_t value) {
  bool ret;
  int wait = 0;
  while (true) {
    // the order of comparison should not be changed
    if (status_.load() == State::USING_PMEMKV) {
      ret = pmemkv_->Put(key, value);
      if (Size() >= PMEMKV_THRESHOLD)
        ChangeToComboTree_();
      break;
    } else if (status_.load() == State::PMEMKV_TO_COMBO_TREE) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    } else if (status_.load() == State::USING_COMBO_TREE) {
      ret = alevel_->Put(key, value);
      if (!ret) continue;
      if (Size() >= EXPANSION_FACTOR * BLEVEL_EXPAND_BUF_KEY * blevel_->Entries())
        ExpandComboTree_();
      ret = true;
      break;
    } else if (status_.load() == State::PREPARE_EXPANDING) {
      if (need_sleep_) {
        std::unique_lock<std::mutex> lock(BLevel::expand_wait_lock);
        if (need_sleep_ && sleeped_threads_ < EXPAND_THREADS) {
          sleeped_threads_++;
          if (sleeped_threads_ == EXPAND_THREADS)
            need_sleep_.store(false);
          LOG(Debug::INFO, "thread waiting for cv");
          BLevel::expand_wait_cv.wait(lock);
          LOG(Debug::INFO, "thread finish waiting for cv");
        }
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        wait++;
      }
      continue;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
#ifndef BRANGE
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
#else
      if (need_sleep_) {
        std::unique_lock<std::mutex> lock(BLevel::expand_wait_lock);
        if (need_sleep_ && sleeped_threads_ < EXPAND_THREADS) {
          sleeped_threads_++;
          if (sleeped_threads_ == EXPAND_THREADS)
            need_sleep_.store(false);
          LOG(Debug::INFO, "thread waiting for cv");
          BLevel::expand_wait_cv.wait(lock);
          LOG(Debug::INFO, "thread finish waiting for cv");
        }
        continue;
      } else {
        int range;
        uint64_t end;
        if (blevel_->IsKeyExpanded(key, range, end))
          ret = blevel_->PutRange(key, value, range, end);
        else {
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
          continue;
          ret = alevel_->Put(key, value);
        }
        if (!ret) continue;
        break;
      }
#endif // BRANGE
    }
  }
  if (wait > 10)
    LOG(Debug::WARNING, "wait too many! %d", wait);
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
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
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
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    } else if (status_.load() == State::USING_COMBO_TREE) {
      ret = alevel_->Delete(key, nullptr);
      break;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }
  }
  return ret;
}

/************************ ComboTree::IterImpl ************************/
class ComboTree::IterImpl {
 public:
  IterImpl(const ComboTree* tree)
    : tree_(tree), biter_(nullptr)
  {
    if (tree_->blevel_ != nullptr) {
      biter_ = new BLevel::Iter(tree_->blevel_);
    } else {
      assert(0);
      biter_ = nullptr;
    }
  }

  IterImpl(const ComboTree* tree, uint64_t start_key)
    : tree_(tree), biter_(nullptr)
  {
    if (tree_->blevel_ != nullptr) {
      uint64_t begin, end;
      tree_->alevel_->GetBLevelRange_(start_key, begin, end);
      biter_ = new BLevel::Iter(tree_->blevel_, start_key, begin, end);
    } else {
      assert(0);
      biter_ = nullptr;
    }
  }

  ~IterImpl() {
    if (biter_)
      delete biter_;
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

/********************* ComboTree::NoSortIterImpl *********************/
class ComboTree::NoSortIterImpl {
 public:
  NoSortIterImpl(const ComboTree* tree)
    : tree_(tree), biter_(nullptr)
  {
    if (tree_->blevel_ != nullptr) {
      biter_ = new BLevel::NoSortIter(tree_->blevel_);
    } else {
      assert(0);
      biter_ = nullptr;
    }
  }

  NoSortIterImpl(const ComboTree* tree, uint64_t start_key)
    : tree_(tree), biter_(nullptr)
  {
    if (tree_->blevel_ != nullptr) {
      uint64_t begin, end;
      tree_->alevel_->GetBLevelRange_(start_key, begin, end);
      biter_ = new BLevel::NoSortIter(tree_->blevel_, start_key, begin, end);
    } else {
      assert(0);
      biter_ = nullptr;
    }
  }

  ~NoSortIterImpl() {
    if (biter_)
      delete biter_;
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
  BLevel::NoSortIter* biter_;
};


/************************ ComboTree::Iter ************************/
ComboTree::Iter::Iter(const ComboTree* tree) : pimpl_(new IterImpl(tree)) {}
ComboTree::Iter::Iter(const ComboTree* tree, uint64_t start_key)
  : pimpl_(new IterImpl(tree, start_key)) {}
uint64_t ComboTree::Iter::key() const   { return pimpl_->key(); }
uint64_t ComboTree::Iter::value() const { return pimpl_->value(); }
bool ComboTree::Iter::next()            { return pimpl_->next(); }
bool ComboTree::Iter::end() const       { return pimpl_->end(); }


/********************* ComboTree::NoSortIter *********************/
ComboTree::NoSortIter::NoSortIter(const ComboTree* tree) : pimpl_(new NoSortIterImpl(tree)) {}
ComboTree::NoSortIter::NoSortIter(const ComboTree* tree, uint64_t start_key)
  : pimpl_(new NoSortIterImpl(tree, start_key)) {}
uint64_t ComboTree::NoSortIter::key() const   { return pimpl_->key(); }
uint64_t ComboTree::NoSortIter::value() const { return pimpl_->value(); }
bool ComboTree::NoSortIter::next()            { return pimpl_->next(); }
bool ComboTree::NoSortIter::end() const       { return pimpl_->end(); }


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