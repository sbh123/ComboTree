#include <mutex>

namespace combotree {

#ifndef NDEBUG
#define NDEBUG

std::mutex log_mutex;

#endif // NDEBUG

}