#include "../../include/exchange/AMQPConnection.h"
#include "../../include/exchange/CommandParser.h"
#include "../../include/exchange/FNV32a.h"
#include "../../include/exchange/Uid.h"
#include "../../include/utils/join.h"

AMQPConnection::AMQPConnection(Logstream& out, const std::string& host, int port,
	const std::string& login, const std::string& password, const std::string& vhost) :
	_out(out), _amqp(_out, host, port, login, password, vhost)
{
	// exchange, routingKey, contentType, replyTo, data
	_amqp.onMessage.connect(boost::bind(&AMQPConnection::handleMessage, this, _1, _2, _3, _4, _5));
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
	_out << "AMQPConnection::handleMessage(...)" << std::endl;

	_out << "We got " << data.size() << " bytes message from AMQP" << std::endl;

	// Let's try to extract hash from message
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
				onPBMessage(type, data.substr(4));

			_out << "We fount bp handler for this type of message. No old parser needed." << std::endl;

			return;
		}

		/*
		for (auto& type : _pbMessageRegisteredTypes)
		{
			uint32_t typeHash = hash(type.first);
			if (typeHash == messageTypeHash.i)
			{
				_out << "We found message type '" << type.first << "' by hash." << std::endl;

				onPBMessage(type.first, data.substr(4));

				_out << "We fount bp handler for this type of message. No old parser needed." << std::endl;

				return;
			}
		}
		*/
	}

	/*
	auto it = _pbMessageRegisteredTypes.find(exchange);
	if (it != _pbMessageRegisteredTypes.end())
	{
		if (it->second)
		{
			onPBMessage(it->first, data);

			_out << "We fount bp handler for this type of message. No old parser needed." << std::endl;

			return;
		}
		else
		{
			_out << "We found registered bp type, but it's only for output." << std::endl;
		}
	}
	*/

	CommandParser commandParser(_out, data);

	switch(commandParser.getType())
	{
	case CommandParser::OK :
		_out << Logstream::INFO << "OK from server." << std::endl;

		break;

	case CommandParser::PING :
		_out << Logstream::INFO << "Ping from server." << std::endl;

		break;

	case CommandParser::ALL :
		_out << Logstream::INFO << "ALL from server." << std::endl;
		{
			const std::vector<std::string>& data = commandParser.getArgs();
			if (data.size() > 0)
			{
				const std::string& uid = data[0];
				_syncRequests[uid] = time(nullptr);
				_out << "Sync '" << uid << "' started." << std::endl;
			}
		}
		break;

	case CommandParser::ALLEND :
		_out << Logstream::INFO << "ALLEND from server." << std::endl;
		{
			const std::vector<std::string>& data = commandParser.getArgs();
			if (data.size() > 0)
			{
				const std::string& uid = data[0];
				auto it = _syncRequests.find(uid);
				if (it != _syncRequests.end())
				{
					_syncRequests.erase(it);
					_out << "Sync '" << uid << "' finished." << std::endl;
				}
			}
		}
		break;

	case CommandParser::DATA :
		{
			if(commandParser.getArgs().size() == 0)
			{
				_out << Logstream::INFO << "There is no data type in data record." << std::endl;

				//addCommand("ERROR[Wrong count of arguments]");

				return;
			}

			const std::string& type = exchange;
			const std::vector<std::string>& data = commandParser.getArgs();
			updateObject(type, data, this);

			//const std::string& type = commandParser.getArgs().at(0);
			//std::vector<std::string> data = commandParser.getArgs();
			//data.erase(data.begin());
			//updateObject(type, data, this);
		}
		break;

	case CommandParser::QUEUE :
		{
			if(commandParser.getArgs().size() == 0)
			{
				_out << Logstream::INFO << "There is no data type in queue record" << std::endl;

				//addCommand("ERROR[Wrong count of arguments]");

				return;
			}

			const std::string& type = exchange;
			const std::vector<std::string>& data = commandParser.getArgs();
			addQueueObject(type, data);


			//const std::string& type = commandParser.getArgs().at(0);
			//std::vector<std::string> data = commandParser.getArgs();
			//data.erase(data.begin());

			//addQueueObject(type, data);
		}
		break;

	case CommandParser::ERR :
		_out << Logstream::INFO << "Error from server!" << std::endl;

		break;

	case CommandParser::GETALL :
		/*
		for (const auto pair : _objectsQueues.end())
		{
			if (pair.second->getDirection() != IObjectsQueue::QUEUE_OUT)
			{
				continue;
			}

			if(pair.second->isEmpty(&connection))
			{
				continue;
			}

			_out << Logstream::INFO << "Sending records from queue '" << pair.second->getType() << "'" << std::endl;

			boost::shared_ptr<IObject> object;
			while((object = pair.second->getObject(&connection)))
			{
				std::string command = "QUEUE[";
				//command += (*it).second->getType() + "|";
				command += ::join(object.get()->serialize(), "|");
				command += "]";
				_out << Logstream::INFO << "Got object update command '" << command << "'" << std::endl;
				//connection.addCommand(command);
				_amqp.send(pair.seconnd->getType(), "", command, false);
			}
		}
		*/

		//for (const auto& pair : _objectsLists.end())
		for(std::map<std::string, IObjectsList*>::iterator it = _objectsLists.begin(), end = _objectsLists.end(); it != end; ++it)
		{
			if (it->second->getType() != exchange)
			{
				continue;
			}

			if (it->second->getSyncType() != IObjectsList::SEND && 
				it->second->getSyncType() != IObjectsList::BIDIRECTIONAL)
			{
				continue;
			}

			_out << Logstream::INFO << "We got GETALL, sending '" << it->second->getType() << "'" << std::endl;

			Uid<16> uidObject;
			const std::string& uid = uidObject.getUid();

			{
				std::string command = std::string("ALL[") + uid + "]";
				_amqp.send(it->second->getType(), "", command, false);
			}

			std::vector<const IObject*> list = it->second->getAllObjects(this);

			for(std::vector<const IObject*>::iterator objIt = list.begin(), objEnd = list.end(); objIt != objEnd; ++objIt)
			{
				std::string command = "DATA[";
				//command += (*it).second->getType() + "|";
				command += ::join((*objIt)->serialize(), "|");
				command += "]";
				_out << Logstream::INFO << "Got object update command '" << command << "'" << std::endl;
				//connection.addCommand(command);
				_amqp.send(it->second->getType(), "", command, false);
				//(*it).second->clearChanges(&connection, *objIt);
				it->second->clearChanges(this, *objIt);
			}

			{
				std::string command = std::string("ALLEND[") + uid + "]";
				_amqp.send(it->second->getType(), "", command, false);
			}

		}

		break;

	default :
		_out << Logstream::INFO << "Unknown command from server!" << std::endl;
		break;
	}

	return;
}

void AMQPConnection::addObjectsList(IObjectsList* objectsList)
{
	_objectsLists[objectsList->getType()] = objectsList;

	objectsList->OnChange.connect(boost::bind(&AMQPConnection::objectsListChangesHandler, this, _1, _2, _3, _4));

	_amqp.addExchange(objectsList->getType(), "fanout", "", true);

	_amqp.send(objectsList->getType(), "", "GETALL[]", false);

	return;
}

void AMQPConnection::objectsListChangesHandler(const IObjectsList::ChangeType& /*changeType*/, IObject& iObject, IObjectsList& iObjectsList, const IConnection* connection)
{
	if(iObjectsList.getSyncType() != IObjectsList::SEND && iObjectsList.getSyncType() != IObjectsList::BIDIRECTIONAL)
	{
		return;
	}

	if(connection == this)
	{
		_out << "Filtering echo for '" << connection << "'" << std::endl;
		return;
	}

	std::string command = "DATA[";
	//command += iObjectsList.getType() + "|";
	command += ::join(iObject.serialize(),"|");
	command += "]";

	_amqp.send(iObjectsList.getType(), "", command, false);

	return;
}

void AMQPConnection::addObjectsQueue(IObjectsQueue* objectsQueue)
{
	_objectsQueues[objectsQueue->getType()] = objectsQueue;

	objectsQueue->newConnectionHandler(this);

	objectsQueue->OnNewObject.connect(boost::bind(&AMQPConnection::objectsQueueChangesHandler, this, _1, _2));

	_amqp.addExchange(objectsQueue->getType(), "fanout", "", true);

	return;
}

void AMQPConnection::objectsQueueChangesHandler(const IConnection* const /*iConnection*/, IObjectsQueue* iObjectsQueue)
{
	if(iObjectsQueue->getDirection() != IObjectsQueue::QUEUE_OUT)
	{
		return;
	}

	if(iObjectsQueue->isEmpty(this))
	{
		return;
	}

	_out << Logstream::INFO << "We got objects in queue '" << iObjectsQueue->getType() << "'" << std::endl;

	boost::shared_ptr<IObject> object;
	while((object = iObjectsQueue->getObject(this)))
	{
		std::string command = "QUEUE[";
		//command += iObjectsQueue->getType() + "|";
		command += ::join(object.get()->serialize(),"|");
		command += "]";

		_amqp.send(iObjectsQueue->getType(), "", command, false);
	}

	return;
}

void AMQPConnection::updateObject(const std::string& listType, const std::vector<std::string>& fields, const IConnection* connection)
{
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

void AMQPConnection::addQueueObject(const std::string& listType, const std::vector<std::string>& fields)
{
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

	if(_objectsQueues.at(listType)->getDirection() == IObjectsQueue::QUEUE_IN)
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

void AMQPConnection::sendPBMessage(const ::google::protobuf::Message& message)
{
	const std::string& type = message.GetTypeName();
	const std::string& exchange = type;

	union
	{
		uint32_t i;
		char c[4];
	} typeHash;

	typeHash.i = hash(type);

	_out << "AMQPConnection: we got pb message for exchange '" << exchange << "', type hash 0x" << std::hex << typeHash.i << std::dec << "." << std::endl;

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

	_amqp.send(exchange, "", data, true);

	return;
}

void AMQPConnection::sendPersonalPBMessage(const std::string& queue, const ::google::protobuf::Message& message)
{
	const std::string& type = message.GetTypeName();

	_out << "AMQPConnection: we got personal pb message with type '" << type << "' for queue '" << queue << "'." << std::endl;

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

	union
	{
		uint32_t i;
		char c[4];
	} typeHash;

	typeHash.i = hash(message.GetTypeName());

	_out << "Adding hash 0x" << std::hex << typeHash.i << std::dec << " to message." << std::endl;

	data.insert(0, typeHash.c, 4);

	_out << "Sending to queue '" << queue << "' pb message '" << message.DebugString() << "'" << std::endl;

	_amqp.send("", queue, data, true);

	return;
}

void AMQPConnection::registerPBMessageHandler(const std::string& type, const boost::function<void (const std::string& type, const std::string& data)>& handler)
{
	onPBMessage.connect(handler);

	uint32_t typeHash = hash(type);

	_pbMessageRegisteredTypes[typeHash] = std::pair<std::string, bool>(type, true);

	_amqp.addExchange(type, "fanout", "", true);

	return;
}

void AMQPConnection::registerPBMessageType(const std::string& type)
{
	uint32_t typeHash = hash(type);

	if (_pbMessageRegisteredTypes.find(typeHash) != _pbMessageRegisteredTypes.end())
	{
		_out << "Pb message type '" << type << "' already registered." << std::endl;

		return;
	}

	_pbMessageRegisteredTypes[typeHash] = std::pair<std::string, bool>(type, false);

	_amqp.addExchange(type, "fanout", "", false);

	return;
}

uint32_t AMQPConnection::hash(const std::string& string)
{
	return fnv32(string);
}

bool AMQPConnection::isSynced()
{
	bool found = false;
	std::vector<std::string> toDelete;

	_out << "There are " << _syncRequests.size() << " sync sessions." << std::endl;

	for (auto request : _syncRequests)
	{
		time_t ti = request.second;
		if (ti - time(nullptr) < 5 * 60)
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
}
