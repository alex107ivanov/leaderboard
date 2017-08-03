#ifndef REDISCONNECTION_H_DEFINED
#define REDISCONNECTION_H_DEFINED

#include <boost/noncopyable.hpp>
#include <boost/signals2.hpp>

#include <google/protobuf/message.h>

#include <stack>
#include <memory>
#include <mutex>
#include <iostream>
#include <condition_variable>

#include <redox.hpp>

class RedisConnection : private boost::noncopyable
{
public:
	struct UserInfo
	{
		size_t userid;
		std::string name;
		size_t place;
		float amount;
		std::vector<std::pair<size_t, float>> top;
		std::vector<std::pair<size_t, float>> around;
	};

	RedisConnection(std::ostream& out, const std::string& host, int port);

	~RedisConnection();

	void join();

	void quit();

	void storeDeal(size_t userid, float amount, size_t doy);

	bool requestUserInfo(size_t userid);

	boost::signals2::signal<void (const UserInfo&)> onUserInfo;

private:
	struct Deal
	{
		size_t userid;
		float amount;
		size_t doy;
	};

	template<typename T>
	class Connection
	{
	public:
		Connection(std::ostream& out, const std::string& host, int port, std::function<void(void)>&& connectedHandler) : 
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

		std::ostream& _out;
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

	void storeDealReplyHandler(redox::Command<int>& command);

	bool storeDeal(const Deal& deal);

	void requestUserInfoReplyHandler(redox::Command<std::vector<std::string>>& command);

/*
	void handleGetObjectsReply(redox::Command<std::vector<std::string>>& command, std::string list);

	void replyHandler(redox::Command<int>& command);

	void getCommandReplyHandler(redox::Command<std::string>& command, std::string list, std::string id);

	void commandReplyHandler(redox::Command<std::string>& command);

	void handleMessage(const std::string& exchange, const std::string& data);

	void updateObject(const std::string& listType, const std::vector<std::string>& fields, const IConnection* connection);

	void addQueueObject(const std::string& listType, const std::vector<std::string>& fields);

	void objectsQueueChangesHandler(const IConnection* const connection, IObjectsQueue* iObjectsQueue);

	void objectsListChangesHandler(const IObjectsList::ChangeType& changeType, IObject& iObject, IObjectsList& iObjectsList, const IConnection* connection);
*/

	std::ostream& _out;

	Connection<redox::Redox> _redox;

	uint32_t hash(const std::string& string);

	//boost::signals2::signal<void (const std::string& /*type*/, const std::string& /*data*/)> onPBMessage;

	std::mutex _mutex;

	bool _quit;
	std::condition_variable _quitCV;


	std::stack<Deal> _deals;
};

#endif
