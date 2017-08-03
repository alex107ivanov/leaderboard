#ifndef USERINFO_H_DEFINED
#define USERINFO_H_DEFINED

#include <cstddef>
#include <string>
#include <vector>

struct UserInfo
{
	size_t userid;
	std::string name;
	size_t place;
	float amount;
	std::vector<std::pair<size_t, float>> top;
	std::vector<std::pair<size_t, float>> around;
};

#endif
