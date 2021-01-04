#include "nvm_alloc.h"
#include "combotree_config.h"

namespace NVM
{
Alloc *common_alloc = nullptr;
Alloc *btree_alloc = nullptr;

int env_init()
{
    common_alloc = new  NVM::Alloc(COMMON_PMEM_FILE, 1024 * 1024 * 1024UL);
    btree_alloc  = new  NVM::Alloc(PMEM_DIR"Fast-Fair", 4 * 1024 * 1024 * 1024UL);
    return 0;
}

void env_exit()
{
    if(common_alloc) delete common_alloc;
    if(btree_alloc) delete btree_alloc;
}
} // namespace NVM
