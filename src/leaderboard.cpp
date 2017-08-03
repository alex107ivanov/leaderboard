#include <iostream>
#include <chrono>
#include <thread>

#include "RedisConnection.h"
#include "AMQPConnection.h"
#include "UserInfo.h"
#include "UserInfoRequester.h"

int main()
{
	//RedisConnection redisConnection(std::cout, "95.213.252.217", 8081);
	RedisConnection redisConnection(std::cout, "127.0.0.1", 6379);

	AMQPConnection amqpConnection(std::cout, "127.0.0.1", 5672, "admin", "password", "/");

	UserInfoRequester userInfoRequester(std::cout, 10);

	userInfoRequester.onRequest.connect([&redisConnection](size_t userid){
		std::cout << "onRequest(" << userid << ")" << std::endl;
		bool result = redisConnection.requestUserInfo(userid);
		if (!result)
		{
			std::cout << "Error requesting userinfo." << std::endl;
		}
	});

	redisConnection.onUserInfo.connect([&amqpConnection](const UserInfo& userInfo){
/*
		std::cout << " ---==( UserInfo )==---" << std::endl;
		std::cout << " userInfo.userid = " << userInfo.userid << std::endl;
		std::cout << " userInfo.name = " << userInfo.name << std::endl;
		std::cout << " userInfo.amount = " << userInfo.amount << std::endl;
		std::cout << " userInfo.place = " << userInfo.place << std::endl;
		std::cout << " userInfo.around:" << std::endl;
		for (const auto& around : userInfo.around)
			std::cout << "   - " << around.first << ", " << around.second << std::endl;
		std::cout << " userInfo.top:" << std::endl;
		for (const auto& top : userInfo.top)
			std::cout << "   - " << top.first << ", " << top.second << std::endl;
*/
		std::cout << " userInfo.userid = " << userInfo.userid << std::endl;

		amqpConnection.sendUserInfo(userInfo);
	});

	amqpConnection.onUserRegistered.connect([](size_t userid, const std::string& name){
		std::cout << "onUserRegistered(" << userid << ", " << name << ")" << std::endl;
	});

	amqpConnection.onUserRenamed.connect([](size_t userid, const std::string& name){
		std::cout << "onUserRenamed(" << userid << ", " << name << ")" << std::endl;
	});

	amqpConnection.onUserDeal.connect([](size_t userid, time_t time, float amount){
		std::cout << "onUserDeal(" << userid << ", " << time << ", "<< amount << ")" << std::endl;
	});

	amqpConnection.onUserDealWon.connect([&redisConnection](size_t userid, time_t time, float amount){
		std::cout << "onUserDealWon(" << userid << ", " << time << ", " << amount << ")" << std::endl;
		struct tm tm;
		gmtime_r(&time, &tm);
		redisConnection.storeDeal(userid, amount, tm.tm_yday);
	});

	amqpConnection.onUserConnected.connect([&userInfoRequester](size_t userid){
		std::cout << "onUserConnected(" << userid << ")" << std::endl;
		userInfoRequester.enableUser(userid);
	});

	amqpConnection.onUserDisconnected.connect([&userInfoRequester](size_t userid){
		std::cout << "onUserDisconnected(" << userid << ")" << std::endl;
		userInfoRequester.disableUser(userid);
	});

	//redisConnection.storeDeal(1000, 123.45f, 5);

	//std::cout << " --- dial stored ?" << std::endl;

	//std::this_thread::sleep_for(std::chrono::seconds(1));

	//bool result = redisConnection.requestUserInfo(1000);
	//bool result = redisConnection.requestUserInfo(14);

	//std::cout << " --- user info requested ? result = " << result << std::endl;
/*
	std::this_thread::sleep_for(std::chrono::seconds(1));

	auto start = std::chrono::system_clock::now();
	for (size_t i = 0; i < 10000; ++i)
	{
		for (size_t j = 0; j < 100; ++j)
		{
			size_t userid = i * 100 + j; //rand() % 10000000 + 1;
			redisConnection.storeDeal(userid, 1.1f, 9);
		}

		size_t userid = i * 100;//rand() % 1000000 + 1;
		bool result = redisConnection.requestUserInfo(userid);
	}
	auto end = std::chrono::system_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

	redisConnection.printStatistics();

	while (!redisConnection.complete())
	{
		std::this_thread::sleep_for(std::chrono::seconds(10));
		redisConnection.printStatistics();
	}

	redisConnection.quit();
	amqpConnection.quit();
*/
	redisConnection.join();
	amqpConnection.join();

/*
	std::this_thread::sleep_for(std::chrono::seconds(1));

	redisConnection.printStatistics();

	std::cout << "Runtime was " << elapsed.count() << " msec." << std::endl;
*/
	return 0;
}
