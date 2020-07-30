#include <iostream>
#include <cassert>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include "combo-tree/combo_tree.h"
#include "iterator.h"
#include "alevel.h"
#include "blevel.h"
#include "manifest.h"
#include "pmemkv.h"

namespace combotree {

ComboTree::ComboTree(std::string pool_dir, size_t pool_size, bool create)
    : pool_dir_(pool_dir), pool_size_(pool_size)
{
  ValidPoolDir_();
  manifest_ = new Manifest(pool_dir_);
  pmemkv_ = new PmemKV(manifest_->PmemKVPath());
}

size_t ComboTree::Size() const {
  if (UsingComboTree_()) {
    return blevel_->Size();
  } else {
    return pmemkv_->Size();
  }
}

void ComboTree::ChangeToComboTree_() {
  pop_ = pmem::obj::pool_base::create(manifest_->ComboTreePath(),
      POOL_LAYOUT, pool_size_, 0666);
  Iterator* iter = pmemkv_->begin();
  blevel_ = new BLevel(pop_, iter, pmemkv_->Size());
  alevel_ = new ALevel(blevel_);
  manifest_->SetIsComboTree(true);
}

bool ComboTree::Insert(uint64_t key, uint64_t value) {

}

bool ComboTree::Update(uint64_t key, uint64_t value) {

}

bool ComboTree::Get(uint64_t key, uint64_t& value) const {

}

bool ComboTree::Delete(uint64_t key) {

}

void ComboTree::ValidPoolDir_() {

}

} // namespace combotree