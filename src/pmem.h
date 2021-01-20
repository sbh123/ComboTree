#pragma once

#include <x86intrin.h>

// cache line clflush
#if __CLWB__
#define clflush _mm_clwb
#define FLUSH_METHOD  "_mm_clwb"
#elif __CLFLUSHOPT__
#define clflush _mm_clflushopt
#define FLUSH_METHOD  "_mm_clflushopt"
#elif __CLFLUSH__
#define clflush _mm_clflush
#define FLUSH_METHOD  "_mm_clflush"
#else
static_assert(0, "cache line clflush not supported!");
#endif

// memory fence
#define fence _mm_sfence
#define FENCE_METHOD  "_mm_sfence"

#define ALWAYS_INLINE inline __attribute__((always_inline))