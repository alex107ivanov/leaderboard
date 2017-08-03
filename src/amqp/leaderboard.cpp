#include <iostream>
#include <chrono>
#include <thread>

#include "RedisConnection.h"

int main()
{
	RedisConnection redisConnection(std::cout, "95.213.252.217", 8081);

	redisConnection.storeDeal(1000, 123.45f, 5);

	std::cout << " --- dial stored ?" << std::endl;

	std::this_thread::sleep_for(std::chrono::seconds(1));

	bool result = redisConnection.requestUserInfo(1000);

	std::cout << " --- user info requested ? result = " << result << std::endl;

	std::this_thread::sleep_for(std::chrono::seconds(1));

	redisConnection.quit();

	redisConnection.join();

	return 0;
}
