#include "nvm_alloc.h"
#include "combotree_config.h"

namespace NVM
{
Alloc *common_alloc = nullptr;
Alloc *btree_alloc = nullptr;
Alloc *data_alloc = nullptr;

#ifdef SERVER
const size_t common_alloc_size = 1024 * 1024 * 1024UL;
const size_t btree_alloc_size  = 20 * 1024 * 1024 * 1024UL;
const size_t data_alloc_size = 20 * 1024 * 1024 * 1024UL;
#else
const size_t common_alloc_size = 1024 * 1024 * 1024UL;
const size_t btree_alloc_size  = 4 * 1024 * 1024 * 1024UL;
const size_t data_alloc_size = 4 * 1024 * 1024 * 1024UL;
#endif
int env_init()
{
    common_alloc = new  NVM::Alloc(COMMON_PMEM_FILE, common_alloc_size);
    btree_alloc  = new  NVM::Alloc(PMEM_DIR"Fast-Fair", btree_alloc_size);
    data_alloc  = new  NVM::Alloc(PMEM_DIR"Fast-Fair", data_alloc_size);
    return 0;
}

void env_exit()
{
    if(data_alloc) delete data_alloc;
    if(btree_alloc) delete btree_alloc;
    if(common_alloc) delete common_alloc;
}
} // namespace NVM
