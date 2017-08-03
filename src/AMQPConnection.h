#ifndef AMQPCONNECTION_H_DEFINED
#define AMQPCONNECTION_H_DEFINED

#include <iostream>
#include <string>
#include <cstdint>

#include <boost/noncopyable.hpp>

#include <google/protobuf/message.h>


#include "AMQP.h"
#include "UserInfo.h"

class AMQPConnection : private boost::noncopyable
{
public:
	AMQPConnection(std::ostream& out, const std::string& host, int port,
		const std::string& login, const std::string& password,
		const std::string& vhost);

	~AMQPConnection();

	void join();

	void quit();

	void sendUserInfo(const UserInfo& userInfo);

	boost::signals2::signal<void (size_t userid, const std::string& name)> onUserRegistered;
	boost::signals2::signal<void (size_t userid, const std::string& name)> onUserRenamed;
	boost::signals2::signal<void (size_t userid, time_t time, float amount)> onUserDeal;
	boost::signals2::signal<void (size_t userid, time_t time, float amount)> onUserDealWon;
	boost::signals2::signal<void (size_t userid)> onUserConnected;
	boost::signals2::signal<void (size_t userid)> onUserDisconnected;

private:
	void handleMessage(const std::string& exchange, const std::string& routingKey, const std::string& contentType, const std::string& replyTo, const std::string& data);

	std::ostream& _out;

	AMQP _amqp;
};

#endif
