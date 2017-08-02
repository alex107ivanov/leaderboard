#ifndef REDISCONNECTION_H_DEFINED
#define REDISCONNECTION_H_DEFINED

//#if defined(_MSC_VER) && _MSC_VER >= 1400 
//#pragma warning(push) 
//#pragma warning(disable:4996) 
//#endif 

//#include <boost/bind.hpp>
#include <boost/noncopyable.hpp>

#include <google/protobuf/message.h>

#include <map>
#include <memory>
#include <mutex>
#include <condition_variable>

#include <redox.hpp>

#include "ObjectsQueue.h"
#include "ObjectsList.h"
#include "IObject.h"

#include "../logstream/Logstream.h"

class RedisConnection : public IConnection, private boost::noncopyable
{
public:
	RedisConnection(Logstream& out, const std::string& host, int port);

	~RedisConnection();

	void join();

	void quit();

	void addObjectsList(IObjectsList* object);

	void addObjectsQueue(IObjectsQueue* object);

	void sendPBMessage(const ::google::protobuf::Message& message);

	void registerPBMessageHandler(const std::string& type, 
		const boost::function<void (const std::string& type, const std::string& data)>& handler);

	void registerPBMessageType(const std::string& type);

	bool isSynced();

private:
	template<typename T>
	class Connection
	{
	public:
		Connection(Logstream& out, const std::string& host, int port, std::function<void(void)>&& connectedHandler) : 
			_out(out),
			_host(host),
			_port(port),
			_connectedHandler(connectedHandler),
			_connected(false),
			_quit(false),
			_connectionThread(std::bind(&Connection<T>::run, this)),
			_sleepSeconds(5)
		{
		}

		bool isConnected()
		{
			_out << "Connection::isConnected()" << std::endl;
			std::unique_lock<std::mutex> lock(_mutex);
			auto status = ((_connection.get() != nullptr) && (_connected));
			_out << "Connection::isConnected(): " << status << std::endl;
			return status;
		}

		T* operator->()
		{
			_out << "Connection::operator->()" << std::endl;
			if (!isConnected())
			{
				_out << "Connection::operator->(): trying to access unconnected connection!" << std::endl;
				return nullptr;
			}
			_out << "Connection::operator->(): connection ok." << std::endl;
			return _connection.get(); 
		}

		void disconnect()
		{
			_out << "Connection::disconnect()" << std::endl;
			{
				_out << "Connection::disconnect(): setting quit flag." << std::endl;
				std::unique_lock<std::mutex> lock(_mutex);
				_quit = true;
			}
			_connectionCV.notify_one();
			_out << "Connection::disconnect(): waiting for connection thread." << std::endl;
			if (_connectionThread.joinable())
				_connectionThread.join();
			else
				_out << "Connection::disconnect(): thread is not joinable :-/" << std::endl;
			_out << "Connection::disconnect(): finished." << std::endl;
		}

		~Connection()
		{
			_out << "Connection::~Connection()" << std::endl;
			disconnect();
			//_connectionThread.join();
			_out << "Connection::~Connection(): finished." << std::endl;
		}

	private:
		void run()
		{
			_out << "Connection::run()" << std::endl;

			std::unique_lock<std::mutex> lock(_mutex);
			while (!_quit)
			{
				if (_connected && !_quit)
				{
					_out << "Connection::run(): connection done, waiting for reconnect or quit signal." << std::endl;
					_connectionCV.wait(lock);
				}

				if (_quit)
				{
					_out << "Connection::run(): got signal for quit." << std::endl;
					break;
				}
				else if (_connected)
				{
					_out << "Connection::run(): got signal for reconnect." << std::endl;
				}

				_connected = false;

				if (_connection.get() != nullptr)
				{
					_out << "Connection::run(): not first connection try. Sleep." << std::endl;
					//lock.unlock();
					std::this_thread::sleep_for(std::chrono::seconds(_sleepSeconds));
					//lock.lock();
					_out << "Connection::run(): sleep done." << std::endl;
				}
				else
				{
					_out << "Connection::run(): first start, no sleep." << std::endl;
				}

				_connection.reset(new T);

				_out << "Connection::run(): trying to connect " << _host << ":" << _port << "." << std::endl;
				//lock.unlock();
				auto result = _connection->connect(_host, _port, std::bind(&Connection<T>::connectionStateChanged, this, std::placeholders::_1));
				//lock.lock();
				if (result)
				{
					_out << "Connection::run(): Connection successfull, calling handler!" << std::endl;
					_connected = true;
					lock.unlock();
					_connectedHandler();
					lock.lock();
				}
				else
				{
					_out << "Connection::run(): Connection error!" << std::endl;
				}
			}
			_out << "Connection::run(): main cycle finished. Removing connection." << std::endl;
			if (_connection.get() != nullptr)
			{
				_out << "Connection::run(): found active connection. Stoping it." << std::endl;
				_connection->stop();
				_connection->wait();
				_out << "Connection::run(): active connection Stopped." << std::endl;
			}
			_connected = false;
			//lock.unlock();
			_connection.reset(nullptr);
			//lock.lock();
			_out << "Connection::run(): finished." << std::endl;
		}

		void connectionStateChanged(int status)
		{
			_out << "Connection::connectionStateChanged(" << status << ")" << std::endl;
			//std::unique_lock<std::mutex> lock(_mutex);
			switch (status)
			{
			case redox::Redox::CONNECTED :
				_out << "Connection::connectionStateChanged(" << status << "): connected." << std::endl;
				break;

			case redox::Redox::NOT_YET_CONNECTED :
			case redox::Redox::DISCONNECTED :
			case redox::Redox::CONNECT_ERROR :
			case redox::Redox::DISCONNECT_ERROR :
			case redox::Redox::INIT_ERROR :
			default :
				_out << "Connection::connectionStateChanged(" << status << "): connection error." << std::endl;
				_connectionCV.notify_one();
				break;
			}
		}

		Logstream& _out;
		std::string _host;
		int _port;
		std::function<void(void)> _connectedHandler;
		std::unique_ptr<T> _connection;
		bool _connected;
		bool _quit;
		std::mutex _mutex;
		std::thread _connectionThread;
		std::condition_variable _connectionCV;
		const size_t _sleepSeconds;
	};

	void connect();

	void redoxConnectedHandler();

	void subscriberConnectedHandler();

	void handleGetObjectsReply(redox::Command<std::vector<std::string>>& command, std::string list);

	void replyHandler(redox::Command<int>& command);

	void getCommandReplyHandler(redox::Command<std::string>& command, std::string list, std::string id);

	void commandReplyHandler(redox::Command<std::string>& command);

	void handleMessage(const std::string& exchange, const std::string& data);

	void updateObject(const std::string& listType, const std::vector<std::string>& fields, const IConnection* connection);

	void addQueueObject(const std::string& listType, const std::vector<std::string>& fields);

	void objectsQueueChangesHandler(const IConnection* const connection, IObjectsQueue* iObjectsQueue);

	void objectsListChangesHandler(const IObjectsList::ChangeType& changeType, IObject& iObject, IObjectsList& iObjectsList, const IConnection* connection);

	Logstream& _out;
	//std::string _host;
	//int _port;

	Connection<redox::Redox> _redox;
	Connection<redox::Subscriber> _subscriber;

	uint32_t hash(const std::string& string);

	std::map<std::string, IObjectsList*> _objectsLists;
	std::map<std::string, IObjectsQueue*> _objectsQueues;

	std::map<uint32_t /*hash*/, std::pair<std::string /*name*/, bool /*input*/>> _pbMessageRegisteredTypes;

	boost::signals2::signal<void (const std::string& /*type*/, const std::string& /*data*/)> onPBMessage;

	//std::map<std::string, time_t> _syncRequests;

	std::mutex _mutex;

	bool _quit;
	//std::mutex _quitMutex;
	std::condition_variable _quitCV;
};

#endif
