#include "../../include/exchange/FNV32a.h"

uint32_t fnv32(const std::string& string)
{
	uint32_t hash = offsetBasis;
	for (size_t i = 0; i < string.length(); ++i)
	{
		hash ^= string[i];
		hash *= fnvPrime;
	}
	return hash;
}
