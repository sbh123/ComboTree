#include "nvm_alloc.h"
#include "statistic.h"
#include "combotree_config.h"
#include "common_time.h"

namespace Common {
    std::map<std::string, Common::Statistic> timers;
    Stat stat;
}

namespace NVM
{
Alloc *common_alloc = nullptr;
Alloc *data_alloc = nullptr;
Stat const_stat;

#ifdef SERVER
const size_t common_alloc_size = 4 *1024 * 1024 * 1024UL;
const size_t data_alloc_size = 50 * 1024 * 1024 * 1024UL;
#else
const size_t common_alloc_size = 1024 * 1024 * 1024UL;
const size_t data_alloc_size = 4 * 1024 * 1024 * 1024UL;
#endif
int env_init()
{
#ifndef USE_MEM
    common_alloc = new  NVM::Alloc(COMMON_PMEM_FILE, common_alloc_size);
#endif
    // data_alloc  = new  NVM::Alloc(PMEM_DIR"data", data_alloc_size);
    Common::timers["ABLevel_times"] = Common::Statistic();
    Common::timers["ALevel_times"] = Common::Statistic();
    Common::timers["BLevel_times"] = Common::Statistic();
    Common::timers["CLevel_times"] = Common::Statistic();
    return 0;
}

int data_init() {
    if(!data_alloc) {
#ifndef USE_MEM
        data_alloc  = new  NVM::Alloc(PMEM_DIR"data", data_alloc_size);
#endif
    }
    return 0;
}

void env_exit()
{
    if(data_alloc) delete data_alloc;
    if(common_alloc) delete common_alloc;
}

void show_stat()
{
    if(data_alloc)  data_alloc->Info();
    if(common_alloc)  common_alloc->Info();
}
} // namespace NVM
