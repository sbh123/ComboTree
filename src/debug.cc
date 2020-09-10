#include <mutex>

namespace combotree {

#ifndef NDEBUG

std::mutex log_mutex;

#endif // NDEBUG

}