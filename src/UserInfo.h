#ifndef USERINFO_H_DEFINED
#define USERINFO_H_DEFINED

#include <cstddef>
#include <string>
#include <vector>

/// Информация о польователе.
struct UserInfo
{
	size_t userid;					///< Id пользователя.
	std::string name;				///< Имя пользователя.
	size_t place;					///< Позиция пользователя в общем списке.
	float amount;					///< Объем прибыльных сделок.
	std::vector<std::pair<size_t, float>> top;	///< TOP 10 пользователей в формате [id пользователя, объем].
	std::vector<std::pair<size_t, float>> around;	///< ближайшые 20 пользователей в формате [id пользователя, объем].
};

#endif
