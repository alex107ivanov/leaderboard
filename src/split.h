#ifndef SPLIT_H_DEFINED
#define SPLIT_H_DEFINED

#include <vector>
#include <string>
#include <sstream>

/// Делит строку на части.
/// @param s Исходная строка.
/// @param delim Разделитель.
/// @param elems Коллекция для созранения результата.
/// @param maxElements Максимальное число частей.
/// @return Коллекция с фрагментами исходной строки.
std::vector<std::string> &split(const std::string &s, const char delim, std::vector<std::string> &elems, unsigned int maxElements = 0);

/// Делит строку на части.
/// @param s Исходная строка.
/// @param delim Разделитель.
/// @param maxElements Максимальное число частей.
/// @return Коллекция с фрагментами исходной строки.
std::vector<std::string> split(const std::string &s, const char delim, unsigned int maxElements = 0);

#endif
