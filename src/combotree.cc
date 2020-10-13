#include <filesystem>
#include <thread>
#include <memory>
#include <iostream>
#include <unistd.h>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include "../include/combotree/combotree.h"
#include "combotree_config.h"
#include "alevel.h"
#include "blevel.h"
#include "manifest.h"
#include "pmemkv.h"
#include "debug.h"

namespace combotree {

uint64_t Config::base_addr_;
uint64_t Config::cur_addr_;

CLevel::MemoryManagement* Config::clevel_mem_;
Slab<CLevel::LeafNode>* Config::clevel_leaf_slab_;
Slab<CLevel>* Config::clevel_slab_;
pmem::obj::pool_base Config::pop_;
std::string Config::pmem_file_;
std::string Config::pmemobj_file_;

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
    manifest_->NewComboTreePath(CLEVEL_PMEMOBJ_FILE_SIZE);

    std::filesystem::remove(manifest_->BLevelPath());
    int is_pmem;
    size_t mapped_len;
    void* blevel_pmem_addr = pmem_map_file(manifest_->BLevelPath().c_str(),
                                pool_size_+64, PMEM_FILE_CREATE | PMEM_FILE_EXCL,
                                0666, &mapped_len, &is_pmem);
    assert(is_pmem == 1);
    if (blevel_pmem_addr == NULL) {
      perror("pmem_map_file");
      exit(1);
    }
    // aligned at 64-bytes
    void* blevel_addr = blevel_pmem_addr;
    if (((uintptr_t)blevel_pmem_addr & (uintptr_t)63) != 0) {
      // not aligned
      blevel_addr = (void*)(((uintptr_t)blevel_addr+64) & ~(uintptr_t)63);
    }
    Config::SetBaseAddr(blevel_addr);

    std::cout << "addr begin: " << blevel_addr << std::endl;
    std::cout << "addr end  : " << (void*)((uint64_t)blevel_addr + pool_size_) << std::endl;

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
    // int old_nice = nice(0);
    // int ret = nice(-old_nice);
    // assert(ret >= 0);
    blevel_->Expansion(old_blevel.get());

    std::shared_ptr<ALevel> new_alevel = std::make_shared<ALevel>(blevel_);
    alevel_ = new_alevel;

    expand_min_key_.store(0);
    expand_max_key_.store(0);

    // remove old pool
    manifest_->NewComboTreePath(CLEVEL_PMEMOBJ_FILE_SIZE);

    // change status
    State s = State::COMBO_TREE_EXPANDING;
    if (!status_.compare_exchange_strong(s, State::USING_COMBO_TREE)) {
      LOG(Debug::ERROR,
          "can not change state from COMBO_TREE_EXPANDING to USING_COMBO_TREE!");
    }

    old_alevel.reset();
    old_blevel.reset();

    LOG(Debug::INFO, "finish expanding combotree. current size is %ld", Size());
    permit_delete_.store(true);
  // });
  // expandion_thread.detach();
}

bool ComboTree::Put(uint64_t key, uint64_t value) {
  Status s;
  int invalid = 0;
  int wait = 0;
  int is_expanding = 0;
  int wait_expanding_finish = 0;
  while (true) {
    // the order of comparison should not be changed
    if (status_.load() == State::USING_PMEMKV) {
      s = pmemkv_->Put(key, value);
      if (s == Status::INVALID) {
        invalid++;
        continue;
      }
      if (Size() >= PMEMKV_THRESHOLD)
        ChangeToComboTree_();
      break;
    } else if (status_.load() == State::PMEMKV_TO_COMBO_TREE) {
      std::this_thread::sleep_for(std::chrono::microseconds(5));
      wait++;
      continue;
    } else if (status_.load() == State::USING_COMBO_TREE) {
      s = alevel_->Put(key, value) ? Status::OK : Status::ALREADY_EXISTS;
      if (s == Status::INVALID) {
        invalid++;
        continue;
      }
      if (Size() >= EXPANSION_FACTOR * blevel_->Entries())
        ExpandComboTree_();
      break;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      if (blevel_->Size() >= EXPANSION_FACTOR * blevel_->Entries()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        wait++;
        wait_expanding_finish++;
        continue;
      } else if (key < expand_min_key_.load()) {
        assert(0);
        // s = blevel_->Put(key, value) ? Status::OK : Status::ALREADY_EXISTS;
        if (s == Status::INVALID) {
          invalid++;
          continue;
        }
      } else if (key >= expand_max_key_.load()) {
        s = alevel_->Put(key, value) ? Status::OK : Status::ALREADY_EXISTS;
        if (s == Status::INVALID) {
          invalid++;
          continue;
        }
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
  assert(s != Status::INVALID);
  if (invalid >= 50)
    LOG(Debug::WARNING, "invalid: %d", invalid);
  if (wait >= 50)
    LOG(Debug::WARNING, "wait: %d, wait finish: %d, is expanding: %d", wait, wait_expanding_finish, is_expanding);
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
      s = alevel_->Get(key, value) ? Status::OK : Status::DOES_NOT_EXIST;
      if (s == Status::INVALID)
        continue;
      break;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      if (key < expand_min_key_.load()) {
        assert(0);
        // s = blevel_->Get(key, value) ? Status::OK : Status::DOES_NOT_EXIST;
        if (s == Status::INVALID)
          continue;
      } else if (key >= expand_max_key_.load()) {
        s = alevel_->Get(key, value) ? Status::OK : Status::DOES_NOT_EXIST;
        if (s == Status::INVALID)
          continue;
      } else {
        assert(0);
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
      s = alevel_->Delete(key, nullptr) ? Status::OK : Status::DOES_NOT_EXIST;
      if (s == Status::INVALID)
        continue;
      break;
    } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
      if (key < expand_min_key_.load()) {
        assert(0);
        // s = blevel_->Delete(key) ? Status::OK : Status::DOES_NOT_EXIST;
        if (s == Status::INVALID)
          continue;
      } else if (key >= expand_max_key_.load()) {
        s = alevel_->Delete(key, nullptr) ? Status::OK : Status::DOES_NOT_EXIST;
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

size_t ComboTree::Scan_(uint64_t min_key, uint64_t max_key, size_t max_size,
                        size_t& count, callback_t callback, void* arg,
                        std::function<uint64_t()> cur_max_key) {
  // while (true) {
  //   if (status_.load() == State::USING_PMEMKV) {
  //     return pmemkv_->Scan(min_key, max_key, max_size, callback, arg);
  //   } else if (status_.load() == State::PMEMKV_TO_COMBO_TREE) {
  //     std::this_thread::sleep_for(std::chrono::microseconds(5));
  //     continue;
  //   } else if (status_.load() == State::USING_COMBO_TREE) {
  //     Status s = blevel_->Scan(min_key, max_key, max_size, count, callback, arg);
  //     if (s == Status::OK)
  //       return count;
  //     if (s == Status::INVALID) {
  //       min_key = max_size == 0 ? min_key : cur_max_key() + 1;
  //       continue;
  //     }
  //   } else if (status_.load() == State::COMBO_TREE_EXPANDING) {
  //     if (min_key < expand_min_key_.load()) {
  //       Status s = blevel_->Scan(min_key, max_key, max_size, count, callback, arg);
  //       if (s == Status::OK)
  //         return count;
  //       if (s == Status::INVALID) {
  //         min_key = max_size == 0 ? min_key : cur_max_key() + 1;
  //         continue;
  //       }
  //     } else if (min_key >= expand_max_key_.load()) {
  //       Status s = alevel_->blevel_->Scan(min_key, max_key, max_size, count, callback, arg);
  //       if (s == Status::OK)
  //         return count;
  //       if (s == Status::INVALID) {
  //         min_key = max_size == 0 ? min_key : cur_max_key() + 1;
  //         continue;
  //       }
  //     } else {
  //       std::this_thread::sleep_for(std::chrono::microseconds(5));
  //       continue;
  //     }
  //   }
  // }
}

void VectorCallback_(uint64_t key, uint64_t value, void* arg) {
  ((std::vector<std::pair<uint64_t, uint64_t>>*)arg)->emplace_back(key, value);
}

size_t ComboTree::Scan(uint64_t min_key, uint64_t max_key, size_t max_size,
                       std::vector<std::pair<uint64_t, uint64_t>>& results) {
  size_t count = 0;
  return Scan_(min_key, max_key, max_size, count, VectorCallback_, &results,
    [&]() {
      return results.empty() ? min_key : results.back().first;
    });
}

void PairCallback_(uint64_t key, uint64_t value, void* arg) {
  Pair* pair = reinterpret_cast<Pair*>(((void**)arg)[0]);
  size_t* count = reinterpret_cast<size_t*>(((void**)arg)[1]);
  pair[*count].key = key;
  pair[*count].value = value;
}

size_t ComboTree::Scan(uint64_t min_key, uint64_t max_key, size_t max_size,
                       Pair* results) {
  size_t count = 0;
  void* callback_arg[2];
  callback_arg[0] = results;
  callback_arg[1] = &count;
  return Scan_(min_key, max_key, max_size, count, PairCallback_, callback_arg,
    [&]() {
      return count == 0 ? min_key : results[count - 1].key;
    });
}

void ArrayCallback_(uint64_t key, uint64_t value, void* arg) {
  uint64_t* results = reinterpret_cast<uint64_t*>(((void**)arg)[0]);
  size_t* count = reinterpret_cast<size_t*>(((void**)arg)[1]);
  uint64_t* last_key = reinterpret_cast<uint64_t*>(((void**)arg)[2]);
  results[*count] = value;
  *last_key = key;
}

size_t ComboTree::Scan(uint64_t min_key, uint64_t max_key, size_t max_size,
                       uint64_t* results) {
  uint64_t last_key;
  size_t count = 0;
  void* callback_arg[3];
  callback_arg[0] = results;
  callback_arg[1] = &count;
  callback_arg[2] = &last_key;
  return Scan_(min_key, max_key, max_size, count, ArrayCallback_, callback_arg,
    [&]() {
      return count == 0 ? min_key : last_key;
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