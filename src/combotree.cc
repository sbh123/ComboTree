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
    std::vector<std::pair<uint64_t,uint64_t>> exist_kv;
    pmemkv_->Scan(0, UINT64_MAX, UINT64_MAX, exist_kv);
    blevel_ = std::make_shared<BLevel>(pop_, exist_kv);
    alevel_ = std::make_shared<ALevel>(blevel_);
    // change manifest first
    manifest_->SetIsComboTree(true);
    State tmp = State::PMEMKV_TO_COMBO_TREE;
    // must change status before wating no ref
    if (!status_.compare_exchange_strong(tmp, State::USING_COMBO_TREE))
      LOG(Debug::ERROR, "can not change state from PMEMKV_TO_COMBO_TREE to USING_COMBO_TREE!");
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
    // the order of comparison should not be changed
    if (status_.load() == State::USING_PMEMKV) {
      s = pmemkv_->Insert(key, value);
      if (s == Status::INVALID)
        continue;
      if (Size() >= PMEMKV_THRESHOLD)
        ChangeToComboTree_();
      break;
    } else if (status_.load() == State::PMEMKV_TO_COMBO_TREE) {
      std::this_thread::sleep_for(std::chrono::microseconds(5));
      continue;
    } else if (status_.load() == State::USING_COMBO_TREE) {
      s = alevel_->Insert(key, value);
      if (s == Status::INVALID)
        continue;
      if (Size() >= EXPANSION_FACTOR * blevel_->EntrySize())
        ExpandComboTree_();
      break;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      if (blevel_->Size() >= EXPANSION_FACTOR * blevel_->EntrySize()) {
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        continue;
      } else if (key < expand_min_key_.load()) {
        s = blevel_->Insert(key, value);
        if (s == Status::INVALID)
          continue;
      } else if (key >= expand_max_key_.load()) {
        s = alevel_->Insert(key, value);
        if (s == Status::INVALID)
          continue;
      } else {
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        continue;
      }
      break;
    }
  }
  assert(s != Status::INVALID);
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
      if (s == Status::INVALID)
        continue;
      break;
    } else if (status_.load() == State::PMEMKV_TO_COMBO_TREE) {
      s = pmemkv_->Get(key, value);
      if (s == Status::INVALID)
        continue;
      break;
    } else if (status_.load() == State::USING_COMBO_TREE) {
      s = alevel_->Get(key, value);
      if (s == Status::INVALID)
        continue;
      break;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      if (key < expand_min_key_.load()) {
        s = blevel_->Get(key, value);
        if (s == Status::INVALID)
          continue;
      } else if (key >= expand_max_key_.load()) {
        s = alevel_->Get(key, value);
        if (s == Status::INVALID)
          continue;
      } else {
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        continue;
      }
      break;
    }
  }
  assert(s != Status::INVALID);
  return s == Status::OK;
}

bool ComboTree::Delete(uint64_t key) {
  Status s;
  while (true) {
    // the order of comparison should not be changed
    if (status_.load() == State::USING_PMEMKV) {
      s = pmemkv_->Delete(key);
      if (s == Status::INVALID)
        continue;
      break;
    } else if (status_.load() == State::PMEMKV_TO_COMBO_TREE) {
      std::this_thread::sleep_for(std::chrono::microseconds(5));
      continue;
    } else if (status_.load() == State::USING_COMBO_TREE) {
      s = alevel_->Delete(key);
      if (s == Status::INVALID)
        continue;
      break;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      if (key < expand_min_key_.load()) {
        s = blevel_->Delete(key);
        if (s == Status::INVALID)
          continue;
      } else if (key >= expand_max_key_.load()) {
        s = alevel_->Delete(key);
        if (s == Status::INVALID)
          continue;
      } else {
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        continue;
      }
      break;
    }
  }
  assert(s != Status::INVALID);
  return s == Status::OK;
}

size_t ComboTree::Scan_(uint64_t min_key, uint64_t max_key, size_t size,
                        std::function<void(uint64_t,uint64_t)> callback,
                        std::function<uint64_t()> cur_max_key) {
  size_t count = 0;
  while (true) {
    if (status_.load() == State::USING_PMEMKV) {
      return pmemkv_->Scan(min_key, max_key, size, callback);
    } else if (status_.load() == State::PMEMKV_TO_COMBO_TREE) {
      std::this_thread::sleep_for(std::chrono::microseconds(5));
      continue;
    } else if (status_.load() == State::USING_COMBO_TREE) {
      Status s = blevel_->Scan(min_key, max_key, size, count, callback);
      if (s == Status::OK)
        return count;
      if (s == Status::INVALID) {
        min_key = size == 0 ? min_key : cur_max_key() + 1;
        continue;
      }
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      if (min_key < expand_min_key_.load()) {
        Status s = blevel_->Scan(min_key, max_key, size, count, callback);
        if (s == Status::OK)
          return count;
        if (s == Status::INVALID) {
          min_key = size == 0 ? min_key : cur_max_key() + 1;
          continue;
        }
      } else if (min_key >= expand_max_key_.load()) {
        Status s = alevel_->blevel_->Scan(min_key, max_key, size, count, callback);
        if (s == Status::OK)
          return count;
        if (s == Status::INVALID) {
          min_key = size == 0 ? min_key : cur_max_key() + 1;
          continue;
        }
      } else {
        std::this_thread::sleep_for(std::chrono::microseconds(5));
        continue;
      }
    }
  }
}

size_t ComboTree::Scan(uint64_t min_key, uint64_t max_key, size_t size,
                       std::vector<std::pair<uint64_t, uint64_t>>& results) {
  return Scan_(min_key, max_key, size,
    [&](uint64_t key, uint64_t value) {
      results.emplace_back(key, value);
    },
    [&]() {
      return results.empty() ? min_key : results.back().first;
    });
}

size_t ComboTree::Scan(uint64_t min_key, uint64_t max_key, size_t size,
                       Pair* results) {
  return Scan_(min_key, max_key, size,
    [&](uint64_t key, uint64_t value) {
      results[size].key = key;
      results[size].value = value;
    },
    [&]() {
      return size == 0 ? min_key : results[size - 1].key;
    });
}

size_t ComboTree::Scan(uint64_t min_key, uint64_t max_key, size_t size,
                       uint64_t* results) {
  uint64_t last_key;
  return Scan_(min_key, max_key, size,
    [&](uint64_t key, uint64_t value) {
      last_key = key;
      results[size] = value;
    },
    [&]() {
      return size == 0 ? min_key : last_key;
    });
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