#include "AMQPConnection.h"

#include "protocol.pb.h"

AMQPConnection::AMQPConnection(std::ostream& out, const std::string& host, int port,
	const std::string& login, const std::string& password, const std::string& vhost) :
	_out(out), _amqp(_out, host, port, login, password, vhost)
{
	// exchange, routingKey, contentType, replyTo, data
	_amqp.onMessage.connect(boost::bind(&AMQPConnection::handleMessage, this, _1, _2, _3, _4, _5));

	_amqp.addExchange("user_registered", "fanout", "", true);
	_amqp.addExchange("user_renamed", "fanout", "", true);
	_amqp.addExchange("user_deal", "fanout", "", true);
	_amqp.addExchange("user_deal_won", "fanout", "", true);
	_amqp.addExchange("user_connected", "fanout", "", true);
	_amqp.addExchange("user_disconnected", "fanout", "", true);

	_amqp.addExchange("user_info", "fanout", "", false);
}

AMQPConnection::~AMQPConnection()
{
	_out << "AMQPConnection::~AMQPConnection()" << std::endl;

	quit();

	join();

	return;
}

void AMQPConnection::quit()
{
	_amqp.quit();
}


void AMQPConnection::join()
{
	_amqp.join();
}

void AMQPConnection::handleMessage(const std::string& exchange, const std::string& routingKey, const std::string& contentType, const std::string& replyTo, const std::string& data)
{
	GOOGLE_PROTOBUF_VERIFY_VERSION;

	_out << "AMQPConnection::handleMessage(...): exchange = '" << exchange << "', routing key = '" << routingKey << "'" << std::endl;

	_out << "We got " << data.size() << " bytes message from AMQP" << std::endl;

	// TODO use hashes
	if (exchange == "user_registered")
	{
		leaderboard::UserRegistered message;
		if (!message.ParseFromString(data))
		{
			_out << "Error parsing message from '" << exchange << "'." << std::endl;
			return;
		}
		onUserRegistered(message.userid(), message.name());
	}
	else if (exchange == "user_renamed")
	{
		
		leaderboard::UserRenamed message;
		if (!message.ParseFromString(data))
		{
			_out << "Error parsing message from '" << exchange << "'." << std::endl;
			return;
		}
		onUserRenamed(message.userid(), message.name());
	}
	else if (exchange == "user_deal")
	{
		
		leaderboard::UserDeal message;
		if (!message.ParseFromString(data))
		{
			_out << "Error parsing message from '" << exchange << "'." << std::endl;
			return;
		}
		onUserDeal(message.userid(), message.time(), message.amount());
	}
	else if (exchange == "user_deal_won")
	{
		
		leaderboard::UserDealWon message;
		if (!message.ParseFromString(data))
		{
			_out << "Error parsing message from '" << exchange << "'." << std::endl;
			return;
		}
		onUserDealWon(message.userid(), message.time(), message.amount());
	}
	else if (exchange == "user_connected")
	{
		
		leaderboard::UserConnected message;
		if (!message.ParseFromString(data))
		{
			_out << "Error parsing message from '" << exchange << "'." << std::endl;
			return;
		}
		onUserConnected(message.userid());
	}
	else if (exchange == "user_disconnected")
	{
		
		leaderboard::UserDisconnected message;
		if (!message.ParseFromString(data))
		{
			_out << "Error parsing message from '" << exchange << "'." << std::endl;
			return;
		}
		onUserDisconnected(message.userid());
	}
	else
	{
		_out << "Got message from unknown exchange '" << exchange << "'." << std::endl;
	}
	return;
}

void AMQPConnection::sendUserInfo(const UserInfo& userInfo)
{
	GOOGLE_PROTOBUF_VERIFY_VERSION;

	std::string command = "test";

	leaderboard::UserInfo userInfoOut;

	userInfoOut.set_userid(userInfo.userid);
	userInfoOut.set_name(userInfo.name);
	userInfoOut.set_place(userInfo.place);
	userInfoOut.set_amount(userInfo.amount);

	for (const auto& top : userInfo.top)
	{
		leaderboard::UserInfo::Place* place = userInfoOut.add_top();
		place->set_place(top.first);
		place->set_amount(top.second);
	}

	for (const auto& around : userInfo.around)
	{
		leaderboard::UserInfo::Place* place = userInfoOut.add_around();
		place->set_place(around.first);
		place->set_amount(around.second);
	}

	std::string serialized;

	if (!userInfoOut.IsInitialized())
	{
		_out << "We got not initialized pb message." << std::endl;
		return;
	}

	if (!userInfoOut.SerializeToString(&serialized))
	{
		_out << "Error while serialize pb message." << std::endl;
		return;
	}

#ifdef _DEBUG
	_out << "Sending pb message '" << userInfoOut.DebugString() << "'" << std::endl;
#endif

	_amqp.send("user_info", "", serialized, false);

	return;
}
