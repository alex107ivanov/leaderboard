#ifndef AMQPCONNECTION_H_DEFINED
#define AMQPCONNECTION_H_DEFINED

#include <boost/bind.hpp>
#include <boost/noncopyable.hpp>

#include <google/protobuf/message.h>

#include <map>

#include "ObjectsQueue.h"
#include "ObjectsList.h"
#include "IObject.h"

#include "AMQP.h"

#include "../logstream/Logstream.h"

class AMQPConnection : public IConnection, private boost::noncopyable
{
public:
	AMQPConnection(Logstream& out, const std::string& host, int port,
		const std::string& login, const std::string& password,
		const std::string& vhost);

	~AMQPConnection();

	void join();

	void quit();

	void addObjectsList(IObjectsList* object);

	void addObjectsQueue(IObjectsQueue* object);

	void sendPBMessage(const ::google::protobuf::Message& message);

	void sendPersonalPBMessage(const std::string& queue, const ::google::protobuf::Message& message);

	void registerPBMessageHandler(const std::string& type, 
		const boost::function<void (const std::string& type, const std::string& data)>& handler);

	void registerPBMessageType(const std::string& type);

	bool isSynced();

private:
	void handleMessage(const std::string& exchange, const std::string& routingKey, const std::string& contentType, const std::string& replyTo, const std::string& data);

	void updateObject(const std::string& listType, const std::vector<std::string>& fields, const IConnection* connection);

	void addQueueObject(const std::string& listType, const std::vector<std::string>& fields);

	void objectsQueueChangesHandler(const IConnection* const connection, IObjectsQueue* iObjectsQueue);

	void objectsListChangesHandler(const IObjectsList::ChangeType& changeType, IObject& iObject, IObjectsList& iObjectsList, const IConnection* connection);

	Logstream& _out;

	uint32_t hash(const std::string& string);

	AMQP _amqp;
	std::map<std::string, IObjectsList*> _objectsLists;
	std::map<std::string, IObjectsQueue*> _objectsQueues;

	std::map<uint32_t /*hash*/, std::pair<std::string /*name*/, bool /*input*/>> _pbMessageRegisteredTypes;

	boost::signals2::signal<void (const std::string& /*type*/, const std::string& /*data*/)> onPBMessage;

	std::map<std::string, time_t> _syncRequests;
};

#endif
