#pragma once

#include <cassert>
#include <cstdio>

namespace combotree {

enum class Debug {
  INFO,
  WARNING,
  ERROR,
};

namespace {

#define ANSI_COLOR_RED      "\x1b[31m"
#define ANSI_COLOR_GREEN    "\x1b[32m"
#define ANSI_COLOR_YELLOW   "\x1b[33m"
#define ANSI_COLOR_BLUE     "\x1b[34m"
#define ANSI_COLOR_MAGENTA  "\x1b[35m"
#define ANSI_COLOR_CYAN     "\x1b[36m"
#define ANSI_COLOR_RESET    "\x1b[0m"

inline const char* level_string__(Debug level) {
  switch (level) {
    case Debug::INFO:
      return ANSI_COLOR_BLUE "INFO" ANSI_COLOR_RESET;
    case Debug::WARNING:
      return ANSI_COLOR_YELLOW "WARNING" ANSI_COLOR_RESET;
    case Debug::ERROR:
      return ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET;
    default:
      return ANSI_COLOR_RED "???" ANSI_COLOR_RESET;
  }
}

}  // anonymous namespace

#ifndef NDEBUG

#define LOG(level, format, ...)                                      \
  printf("%s " ANSI_COLOR_GREEN "%s: " ANSI_COLOR_RESET format "\n", \
         level_string__(level), __FUNCTION__, ##__VA_ARGS__)

#define debug_assert(...) assert(__VA_ARGS__)

#elif defined(NDEBUG)

#define LOG(level, format, ...)

#define debug_assert()

#endif  // NDEBUG

}  // namespace combotree