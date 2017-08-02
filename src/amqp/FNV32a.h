#ifndef FNV32_H_DEFINED
#define FNV32_H_DEFINED

#include <string>

uint32_t fnv32(const std::string& string);

static const uint32_t fnvPrime = 16777619u;
static const uint32_t offsetBasis = 2166136261u;

#endif
