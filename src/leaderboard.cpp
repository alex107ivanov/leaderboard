#include <iostream>
#include <chrono>
#include <thread>

#include "RedisConnection.h"
#include "AMQPConnection.h"
#include "UserInfo.h"
#include "UserInfoRequester.h"

int main()
{
	RedisConnection redisConnection(std::cout, "127.0.0.1", 6379);

	AMQPConnection amqpConnection(std::cout, "127.0.0.1", 5672, "admin", "password", "/");

	UserInfoRequester userInfoRequester(std::cout, 60);

	userInfoRequester.onRequest.connect([&redisConnection](size_t userid){
		std::cout << "onRequest(" << userid << ")" << std::endl;
		bool result = redisConnection.requestUserInfo(userid);
		if (!result)
		{
			std::cout << "Error requesting userinfo." << std::endl;
		}
	});

	redisConnection.onUserInfo.connect([&amqpConnection](const UserInfo& userInfo){
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

	userInfoRequester.join();
	redisConnection.join();
	amqpConnection.join();


	return 0;
}
