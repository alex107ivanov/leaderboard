#include "../../include/exchange/RedisConnection.h"

#include "../../include/exchange/CommandParser.h"
#include "../../include/exchange/FNV32a.h"
#include "../../include/exchange/Uid.h"
#include "../../include/utils/join.h"

RedisConnection::RedisConnection(Logstream& out, const std::string& host, int port) :
	_out(out), 
	_redox(out, host, port, std::bind(&RedisConnection::redoxConnectedHandler, this)), 
	_subscriber(out, host, port, std::bind(&RedisConnection::subscriberConnectedHandler, this)),
	_quit(false)
{
	_out << "RedisConnection::RedisConnection()" << std::endl;
}

void RedisConnection::redoxConnectedHandler()
{
	_out << "RedisConnection::redoxConnectedHandler()" << std::endl;

	//std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			_out << "RedisConnection::redoxConnectedHandler(): got quit." << std::endl;
			return;
		}
	}

	// send all lists
	for (const auto& list : _objectsLists)
	{
		const auto& objectsList = list.second;
		// if direction is OUT
		if (objectsList->getSyncType() != IObjectsList::ACCEPT)
		{
			std::vector<std::string> cmd;
			cmd.push_back("HMSET");
			cmd.push_back(objectsList->getType());

			_out << Logstream::INFO << "RedisConnection::redoxConnectedHandler(): Sending full list " << objectsList->getType() << " state." << std::endl;
			std::vector<const IObject*> oList = objectsList->getAllObjects(this);
			for (auto objIt = oList.begin(), objEnd = oList.end(); objIt != objEnd; ++objIt)
			{
				const auto& uid = (*objIt)->getUid();
				auto data = ::join((*objIt)->serialize(), "|");
				objectsList->clearChanges(this, *objIt);
				cmd.push_back(uid);
				cmd.push_back(data);
			}
			if (cmd.size() > 2)
			{
				if (_redox.isConnected())
				{
					_redox->command<std::string>(cmd, std::bind(&RedisConnection::commandReplyHandler, this, std::placeholders::_1));
					_redox->publish("list:" + objectsList->getType(), "*");
				}
			}
			else
			{
				_out << Logstream::INFO << "RedisConnection::redoxConnectedHandler(): list " << objectsList->getType() << " is empty." << std::endl;
			}
		}

		if (objectsList->getSyncType() != IObjectsList::SEND)
		{
			_out << Logstream::INFO << "Requesting full list " << objectsList->getType() << " state." << std::endl;
			_redox->command<std::vector<std::string>>({"HVALS", objectsList->getType()}, std::bind(&RedisConnection::handleGetObjectsReply, this, std::placeholders::_1, objectsList->getType()));
		}
	}
	return;
}

void RedisConnection::subscriberConnectedHandler()
{
	_out << "RedisConnection::subscriberConnectedHandler()" << std::endl;

	//std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			_out << "RedisConnection::subscriberConnectedHandler(): got quit." << std::endl;
			return;
		}
	}

	// subscribe for all lists & queues
	for (const auto& list : _objectsLists)
	{
		const auto& objectsList = list.second;
		if(objectsList->getSyncType() != IObjectsList::SEND)
		{
			_out << "RedisConnection::subscriberConnectedHandler(): subscribing for list " << objectsList->getType() << " changes." << std::endl;
			if (_subscriber.isConnected())
				_subscriber->subscribe("list:" + objectsList->getType(), std::bind(&RedisConnection::handleMessage, this, std::placeholders::_1, std::placeholders::_2));
		}
		//_redox->command<std::vector<std::string>>({"HVALS", objectsList->getType()}, std::bind(&RedisConnection::handleGetObjectsReply, this, std::placeholders::_1, objectsList->getType()));
	}
	//
	for (const auto& queue : _objectsQueues)
	{
		const auto& objectsQueue = queue.second;
		if(objectsQueue->getDirection() != IObjectsQueue::QUEUE_OUT)
		{
			_out << "RedisConnection::subscriberConnectedHandler(): subscribing for queue " << objectsQueue->getType() << "." << std::endl;
			if (_subscriber.isConnected())
				_subscriber->subscribe("queue:" + objectsQueue->getType(), std::bind(&RedisConnection::handleMessage, this, std::placeholders::_1, std::placeholders::_2));
		}
	}
	//
	for (const auto& pbType : _pbMessageRegisteredTypes)
	{
		const auto& type = pbType.second.first;
		_out << "RedisConnection::subscriberConnectedHandler(): subscribing for pb messages type " << type << "." << std::endl;
		if (_subscriber.isConnected())
			_subscriber->subscribe("pb:" + type, std::bind(&RedisConnection::handleMessage, this, std::placeholders::_1, std::placeholders::_2));
	}
	//
	return;
}

RedisConnection::~RedisConnection()
{
	_out << "RedisConnection::~RedisConnection()" << std::endl;

	//std::unique_lock<std::mutex> lock(_quitMutex);

	_redox.disconnect();

	_subscriber.disconnect();

	quit();

	join();

	return;
}

void RedisConnection::quit()
{
	_out << "RedisConnection::quit()" << std::endl;

	//std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		_quit = true;
	}

	_quitCV.notify_one();

//	boost::mutex::scoped_lock lock(_mutex);

//	_redox.reset(nullptr);
//	_subscriber.reset(nullptr);
//	_amqp.quit();

	_out << "RedisConnection::quit(): finish." << std::endl;
}

void RedisConnection::join()
{
	_out << "RedisConnection::join()" << std::endl;

	//std::unique_lock<std::mutex> lock(_quitMutex);
	std::unique_lock<std::mutex> lock(_mutex);
	while (!_quit)
		_quitCV.wait(lock);

	_out << "RedisConnection::join(): finish." << std::endl;
}

void RedisConnection::handleMessage(const std::string& topic, const std::string& data)
{
	//_out << "RedisConnection::handleMessage(" << topic << ", " << data << ")" << std::endl;
	_out << "RedisConnection::handleMessage(" << topic << ", data)" << std::endl;

	//std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			//_out << "RedisConnection::handleMessage(" << topic << ", " << data << "): got quit." << std::endl;
			_out << "RedisConnection::handleMessage(" << topic << ", data): got quit." << std::endl;
			return;
		}
	}

	auto parts = ::split(topic, ':', 2);
	if (parts.size() != 2)
	{
		_out << "RedisConnection::handleMessage: error parsing topic." << std::endl;
		return;
	}

	const auto type = parts[0];

	_out << "RedisConnection::handleMessage: message type is " << type << std::endl;

	if (type == "pb")
	{
		_out << "RedisConnection::handleMessage: message type is pb." << std::endl;
		if (data.size() >= 4)
		{
			union
			{
				uint32_t i;
				char c[4];
			} messageTypeHash;

			for (size_t i = 0; i < 4; ++i)
			{
				messageTypeHash.c[i] = data[i];
			}

			_out << "Got message with hash 0x" << std::hex << messageTypeHash.i << std::dec << "." << std::endl;

			auto it = _pbMessageRegisteredTypes.find(messageTypeHash.i);
			if (it != _pbMessageRegisteredTypes.end())
			{
				const std::string& type = it->second.first;
				bool canReceive = it->second.second;

				_out << "We found message type '" << type << "' by hash, we " << (canReceive ? "can" : "can't") << " receive it." << std::endl;

				if (canReceive)
				{
					//boost::mutex::scoped_lock lock(_mutex);
//					lock.unlock();
					onPBMessage(type, data.substr(4));
//					lock.lock();
				}

				_out << "We fount bp handler for this type of message. No old parser needed." << std::endl;

				return;
			}
			else
			{
				_out << "RedisConnection::handleMessage: there is not handler registered for this type of messages." << std::endl;
			}
		}
		else
		{
			_out << "RedisConnection::handleMessage: message body is too short." << std::endl;
		}
	}
	else if (type == "queue")
	{
		const auto& queue = parts[1];
		_out << "RedisConnection::handleMessage: we got object for queue " << queue << "." << std::endl;
		const auto& d = ::split(data, '|');

//		lock.unlock();
		addQueueObject(queue, d);
//		lock.lock();

	}
	else if (type == "list")
	{
		//boost::mutex::scoped_lock lock(_mutex);

		const auto& list = parts[1];
		const auto& uid = data;
		_out << "RedisConnection::handleMessage: we got update for " << list << ":" << uid << "." << std::endl;
		if (_redox.isConnected())
		{
			if (uid == "*")
			{
				_redox->command<std::vector<std::string>>({"HVALS", list}, std::bind(&RedisConnection::handleGetObjectsReply, this, std::placeholders::_1, list));
			}
			else
			{
				_redox->command<std::string>({"HGET", list, uid}, std::bind(&RedisConnection::getCommandReplyHandler, this, std::placeholders::_1, list, uid));
			}
		}
	}
	else
	{
		_out << "RedisConnection::handleMessage: message type is unknown." << std::endl;
	}
	return;
}

void RedisConnection::commandReplyHandler(redox::Command<std::string>& command)
{
	_out << "RedisConnection::commandReplyHandler(redox::Command<std::string>& command)" << std::endl;
//	std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			_out << "RedisConnection::commandReplyHandler(redox::Command<std::string>& command): got quit." << std::endl;
			return;
		}
	}

	if(!command.ok())
	{
		_out << "Wrong answer on command." << std::endl;
		return;
	}
	_out << "Command complete. " << command.cmd() << ": " << command.reply() << std::endl; 
	return;
}

void RedisConnection::getCommandReplyHandler(redox::Command<std::string>& command, std::string list, std::string id)
{
	_out << "RedisConnection::getCommandReplyHandler(redox::Command<std::string>& command, std::string list, std::string id)" << std::endl;
	//std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			_out << "RedisConnection::getCommandReplyHandler(redox::Command<std::string>& command, std::string list, std::string id): got quit." << std::endl;
			return;
		}
	}

	if(!command.ok())
	{
		_out << "Wrong answer on command." << std::endl;
		return;
	}
	_out << "Command complete. We were fetching " << list << ":" << id << ". " << command.cmd() << ": " << command.reply() << std::endl; 
	const auto& data = ::split(command.reply(), '|');

//	lock.unlock();
	updateObject(list, data, this);
//	lock.lock();

	return;
}

void RedisConnection::addObjectsList(IObjectsList* objectsList)
{
	_out << "RedisConnection::addObjectsList(" << objectsList->getType() << ")" << std::endl;

//	std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			_out << "RedisConnection::addObjectsList(" << objectsList->getType() << "): got quit." << std::endl;
			return;
		}
	}

	_objectsLists[objectsList->getType()] = objectsList;

	objectsList->OnChange.connect(boost::bind(&RedisConnection::objectsListChangesHandler, this, _1, _2, _3, _4));

	if (_subscriber.isConnected())
		_subscriber->subscribe("list:" + objectsList->getType(), std::bind(&RedisConnection::handleMessage, this, std::placeholders::_1, std::placeholders::_2));

	if (_redox.isConnected())
	{
		if (objectsList->getSyncType() != IObjectsList::ACCEPT)
		{
			std::vector<std::string> cmd;
			cmd.push_back("HMSET");
			cmd.push_back(objectsList->getType());

			_out << Logstream::INFO << "RedisConnection::addObjectsList(): Sending full list " << objectsList->getType() << " state." << std::endl;
			std::vector<const IObject*> oList = objectsList->getAllObjects(this);
			for (auto objIt = oList.begin(), objEnd = oList.end(); objIt != objEnd; ++objIt)
			{
				const auto& uid = (*objIt)->getUid();
				auto data = ::join((*objIt)->serialize(), "|");
				objectsList->clearChanges(this, *objIt);
				cmd.push_back(uid);
				cmd.push_back(data);
			}
			if (cmd.size() > 2)
			{
				_redox->command<std::string>(cmd, std::bind(&RedisConnection::commandReplyHandler, this, std::placeholders::_1));
				_redox->publish("list:" + objectsList->getType(), "*");
			}
			else
			{
				_out << Logstream::INFO << "RedisConnection::addObjectsList(): list " << objectsList->getType() << " is empty." << std::endl;
			}
		}

		if (objectsList->getSyncType() != IObjectsList::SEND)
		{
			_out << Logstream::INFO << "RedisConnection::addObjectsList(): Subscribing for list " << objectsList->getType() << " changes." << std::endl;
			_redox->command<std::vector<std::string>>({"HVALS", objectsList->getType()}, std::bind(&RedisConnection::handleGetObjectsReply, this, std::placeholders::_1, objectsList->getType()));
		}
	}

	return;
}

void RedisConnection::handleGetObjectsReply(redox::Command<std::vector<std::string>>& command, std::string list)
{
	_out << "RedisConnection::handleGetObjectsReply(redox::Command<std::vector<std::string>>& command, std::string list)" << std::endl;
//	std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			_out << "RedisConnection::handleGetObjectsReply(redox::Command<std::vector<std::string>>& command, std::string list): got quit." << std::endl;
			return;
		}
	}

	if (!command.ok())
	{
		_out << "Error on get all objects." << std::endl;
		return;
	}

	for (const auto& str : command.reply())
	{
		_out << "got objects reply [" << command.cmd() << "]: " << str << std::endl;
		const auto& data = ::split(str, '|');

//		lock.unlock();
		updateObject(list, data, this);
//		lock.lock();

	}
	return;
}

void RedisConnection::objectsListChangesHandler(const IObjectsList::ChangeType& /*changeType*/, IObject& iObject, IObjectsList& iObjectsList, const IConnection* connection)
{
	_out << "RedisConnection::objectsListChangesHandler(const IObjectsList::ChangeType& /*changeType*/, IObject& iObject, IObjectsList& iObjectsList, const IConnection* connection)" << std::endl;
//	std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			_out << "RedisConnection::objectsListChangesHandler(const IObjectsList::ChangeType& /*changeType*/, IObject& iObject, IObjectsList& iObjectsList, const IConnection* connection): got quit." << std::endl;
			return;
		}
	}

	if (iObjectsList.getSyncType() != IObjectsList::SEND && iObjectsList.getSyncType() != IObjectsList::BIDIRECTIONAL)
	{
		return;
	}

	if (connection == this)
	{
		_out << "Filtering echo for '" << connection << "'" << std::endl;
		return;
	}

	std::string list = iObjectsList.getType();
	std::string uid = iObject.getUid();
	std::string value = ::join(iObject.serialize(), "|");

	//boost::mutex::scoped_lock lock(_mutex);

	if (_redox.isConnected())
	{
		_redox->command<int>({"HSET", list, uid, value}, std::bind(&RedisConnection::replyHandler, this, std::placeholders::_1));
		_redox->publish("list:" + list, uid);
	}

	return;
}

void RedisConnection::replyHandler(redox::Command<int>& command)
{
	_out << "RedisConnection::replyHandler(redox::Command<int>& command)" << std::endl;

//	std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			_out << "RedisConnection::replyHandler(redox::Command<int>& command): got quit." << std::endl;
			return;
		}
	}

	if(!command.ok())
	{
		_out << "Wrong answer on command." << std::endl;
		return;
	}
	_out << "Command complete. " << command.cmd() << ": " << command.reply() << std::endl;
	return;
}

void RedisConnection::addObjectsQueue(IObjectsQueue* objectsQueue)
{
	_out << "RedisConnection::addObjectsQueue(" << objectsQueue->getType() << ")" << std::endl;

//	std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			_out << "RedisConnection::addObjectsQueue(" << objectsQueue->getType() << "): got quit." << std::endl;
			return;
		}
	}

	_objectsQueues[objectsQueue->getType()] = objectsQueue;

	objectsQueue->newConnectionHandler(this);

	objectsQueue->OnNewObject.connect(boost::bind(&RedisConnection::objectsQueueChangesHandler, this, _1, _2));

	//boost::mutex::scoped_lock lock(_mutex);

	if (_subscriber.isConnected())
	{
		if(objectsQueue->getDirection() != IObjectsQueue::QUEUE_OUT)
		{
			_out << "RedisConnection::addObjectsQueue(): Subscribing for queue " << objectsQueue->getType() << " changes." << std::endl;
			_subscriber->subscribe("queue:" + objectsQueue->getType(), std::bind(&RedisConnection::handleMessage, this, std::placeholders::_1, std::placeholders::_2));
		}
	}

	return;
}

void RedisConnection::objectsQueueChangesHandler(const IConnection* const /*iConnection*/, IObjectsQueue* iObjectsQueue)
{
	_out << "RedisConnection::objectsQueueChangesHandler(const IConnection* const /*iConnection*/, IObjectsQueue* iObjectsQueue)" << std::endl;
//	std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			_out << "RedisConnection::objectsQueueChangesHandler(const IConnection* const /*iConnection*/, IObjectsQueue* iObjectsQueue): got quit." << std::endl;
			return;
		}
	}

	if(iObjectsQueue->getDirection() != IObjectsQueue::QUEUE_OUT)
	{
		return;
	}

	if (iObjectsQueue->isEmpty(this))
	{
		return;
	}

	_out << Logstream::INFO << "We got objects in queue '" << iObjectsQueue->getType() << "'" << std::endl;

	boost::shared_ptr<IObject> object;
	while ((object = iObjectsQueue->getObject(this)))
	{
		//boost::mutex::scoped_lock lock(_mutex);

		auto command = ::join(object.get()->serialize(),"|");
		if (_redox.isConnected())
		{
			_redox->publish("queue:" + iObjectsQueue->getType(), command);
		}
	}

	return;
}

void RedisConnection::updateObject(const std::string& listType, const std::vector<std::string>& fields, const IConnection* connection)
{
	_out << "RedisConnection::updateObject(const std::string& listType, const std::vector<std::string>& fields, const IConnection* connection)" << std::endl;

	//std::unique_lock<std::mutex> lock(_quitMutex);

	if(fields.size() == 0)
	{
		_out << Logstream::INFO << "We got vector with no values!" << std::endl;
		return;
	}

	if(_objectsLists.find(listType) == _objectsLists.end())
	{
		_out << Logstream::INFO << "Objects list with type '" << listType << "' not found!" << std::endl;
		return;
	}

	if(_objectsLists.at(listType)->getSyncType() == IObjectsList::ACCEPT || _objectsLists.at(listType)->getSyncType() == IObjectsList::BIDIRECTIONAL)
	{
		_objectsLists.at(listType)->updateObject(fields, IObject::EXCHANGE, connection);
	}
	else
	{
		_out << Logstream::INFO << "Mode of objects list is not alowing to sync it" << std::endl;
	}

	return;
}

void RedisConnection::addQueueObject(const std::string& listType, const std::vector<std::string>& fields)
{
	_out << "RedisConnection::addQueueObject(const std::string& listType, const std::vector<std::string>& fields)" << std::endl;

	//std::unique_lock<std::mutex> lock(_quitMutex);

	if(fields.size() == 0)
	{
		_out << Logstream::INFO << "We got vector with no values!" << std::endl;
		return;
	}

	if(_objectsQueues.find(listType) == _objectsQueues.end())
	{
		_out << Logstream::INFO << "Objects queue with type '" << listType << "' not found!" << std::endl;
		return;
	}

	if (_objectsQueues.at(listType)->getDirection() == IObjectsQueue::QUEUE_IN)
	{
		_objectsQueues.at(listType)->putObject(this, fields);
		//_objectsQueues.at(listType)->putObject(nullptr, fields);
	}
	else
	{
		_out << Logstream::INFO << "Direction of objects queue is not alowing to create objects" << std::endl;
	}

	return;
}

void RedisConnection::sendPBMessage(const ::google::protobuf::Message& message)
{
	_out << "RedisConnection::sendPBMessage(const ::google::protobuf::Message& message)" << std::endl;

//	std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			_out << "RedisConnection::sendPBMessage(const ::google::protobuf::Message& message): got quit." << std::endl;
			return;
		}
	}

	const std::string& type = message.GetTypeName();
	const std::string& exchange = type;

	union
	{
		uint32_t i;
		char c[4];
	} typeHash;

	typeHash.i = hash(type);

	_out << "RedisConnection: we got pb message for exchange '" << exchange << "', type hash 0x" << std::hex << typeHash.i << std::dec << "." << std::endl;

	if (_pbMessageRegisteredTypes.find(typeHash.i) == _pbMessageRegisteredTypes.end())
	{
		_out << "We got no exchange registered for pb message type '" << exchange << "'" << std::endl;

		return;
	}

	std::string data;

	if (!message.IsInitialized())
	{
		_out << "We got not initialized pb message." << std::endl;
		return;
	}

	if (!message.SerializeToString(&data))
	{
		_out << "Error while serialize pb message." << std::endl;
		return;
	}

	_out << "Adding hash 0x" << std::hex << typeHash.i << std::dec << " to message." << std::endl;

	data.insert(0, typeHash.c, 4);

	_out << "Sending to exchange '" << exchange << "' pb message '" << message.DebugString() << "'" << std::endl;

	//boost::mutex::scoped_lock lock(_mutex);

	//_amqp.send(exchange, "", data, true);
	if (_redox.isConnected())
		_redox->publish("pb:" + exchange, data);

	return;
}

void RedisConnection::registerPBMessageHandler(const std::string& type, const boost::function<void (const std::string& type, const std::string& data)>& handler)
{
	_out << "RedisConnection::registerPBMessageHandler type = '" << type << "'." << std::endl;

//	std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			_out << "RedisConnection::registerPBMessageHandler type = '" << type << "': got quit." << std::endl;
			return;
		}
	}

	onPBMessage.connect(handler);

	uint32_t typeHash = hash(type);

	_pbMessageRegisteredTypes[typeHash] = std::pair<std::string, bool>(type, true);

	//boost::mutex::scoped_lock lock(_mutex);

	//_amqp.addExchange(type, "fanout", "", true);
	if (_subscriber.isConnected())
		_subscriber->subscribe("pb:" + type, std::bind(&RedisConnection::handleMessage, this, std::placeholders::_1, std::placeholders::_2));

	return;
}

void RedisConnection::registerPBMessageType(const std::string& type)
{
	_out << "RedisConnection::registerPBMessageType type = '" << type << "'." << std::endl;

//	std::unique_lock<std::mutex> lock(_quitMutex);
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
			_out << "RedisConnection::registerPBMessageType type = '" << type << "': got quit." << std::endl;
			return;
		}
	}

	uint32_t typeHash = hash(type);

	if (_pbMessageRegisteredTypes.find(typeHash) != _pbMessageRegisteredTypes.end())
	{
		_out << "Pb message type '" << type << "' already registered." << std::endl;

		return;
	}

	_pbMessageRegisteredTypes[typeHash] = std::pair<std::string, bool>(type, false);

	//boost::mutex::scoped_lock lock(_mutex);

	//_amqp.addExchange(type, "fanout", "", false);
	if (_subscriber.isConnected())
		_subscriber->subscribe("pb:" + type, std::bind(&RedisConnection::handleMessage, this, std::placeholders::_1, std::placeholders::_2));

	return;
}

uint32_t RedisConnection::hash(const std::string& string)
{
	return fnv32(string);
}

bool RedisConnection::isSynced()
{
//	std::unique_lock<std::mutex> lock(_quitMutex);
	std::unique_lock<std::mutex> lock(_mutex);

	return true;
/*
	bool found = false;
	std::vector<std::string> toDelete;

	_out << "There are " << _syncRequests.size() << " sync sessions." << std::endl;

	for (auto request : _syncRequests)
	{
		time_t ti = request.second;
		if (time(nullptr) - ti < 5 * 60)
			found = true;
		else
			toDelete.push_back(request.first);
	}

	for (const auto& uid : toDelete)
	{
		_out << "Deleting too old sync session '" << uid << "'." << std::endl;
		auto it = _syncRequests.find(uid);
		if (it != _syncRequests.end())
			_syncRequests.erase(it);
	}

	return !found;
*/
}
