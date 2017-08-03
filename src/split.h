#ifndef SPLIT_H_DEFINED
#define SPLIT_H_DEFINED

#include <vector>
#include <string>
#include <sstream>

std::vector<std::string> &split(const std::string &s, const char delim, std::vector<std::string> &elems, unsigned int maxElements = 0);

std::vector<std::string> split(const std::string &s, const char delim, unsigned int maxElements = 0);

#endif
