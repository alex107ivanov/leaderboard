#ifndef REDISCONNECTION_H_DEFINED
#define REDISCONNECTION_H_DEFINED

#include <stack>
#include <memory>
#include <mutex>
#include <iostream>
#include <condition_variable>

#include <boost/noncopyable.hpp>
#include <boost/signals2.hpp>

#include <redox.hpp>

#include "UserInfo.h"

/// Подключение к серверу redis.
class RedisConnection : private boost::noncopyable
{
public:
	/// Конструктор.
	/// @param out Поток для вывода сообщений.
	/// @param host Адрес сервера.
	/// @param port Порт сервера.
	RedisConnection(std::ostream& out, const std::string& host, int port);

	/// Деструктор.
	~RedisConnection();

	/// Возвращает управление по завершении работы.
	void join();

	/// Инициирует завершение работы.
	void quit();

	/// Сохраняет данные о сделке.
	/// @param userid Id пользователя.
	/// @param amount Объем сделки.
	/// @param doy День года.
	void storeDeal(size_t userid, float amount, size_t doy);

	/// Запрашивает информацию о пользователе.
	/// @param userid Id пользователя.
	bool requestUserInfo(size_t userid);

	/// Выводит статистику соединения.
	void printStatistics();

	/// Возвращает признак получения ответов на все запросы.
	bool complete();

	/// Сигнал о сформированных данных о пользователе.
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
#ifdef _DEBUG
			_out << "Connection::isConnected()" << std::endl;
#endif
			std::unique_lock<std::mutex> lock(_mutex);
			auto status = ((_connection.get() != nullptr) && (_connected));
#ifdef _DEBUG
			_out << "Connection::isConnected(): " << status << std::endl;
#endif
			return status;
		}

		T* operator->()
		{
#ifdef _DEBUG
			_out << "Connection::operator->()" << std::endl;
#endif
			if (!isConnected())
			{
#ifdef _DEBUG
				_out << "Connection::operator->(): trying to access unconnected connection!" << std::endl;
#endif
				return nullptr;
			}
#ifdef _DEBUG
			_out << "Connection::operator->(): connection ok." << std::endl;
#endif
			return _connection.get(); 
		}

		void disconnect()
		{
#ifdef _DEBUG
			_out << "Connection::disconnect()" << std::endl;
#endif
			{
#ifdef _DEBUG
				_out << "Connection::disconnect(): setting quit flag." << std::endl;
#endif
				std::unique_lock<std::mutex> lock(_mutex);
				_quit = true;
			}
			_connectionCV.notify_one();
#ifdef _DEBUG
			_out << "Connection::disconnect(): waiting for connection thread." << std::endl;
#endif
			if (_connectionThread.joinable())
			{
				_connectionThread.join();
			}
			else
			{
#ifdef _DEBUG
				_out << "Connection::disconnect(): thread is not joinable :-/" << std::endl;
#endif
			}
#ifdef _DEBUG
			_out << "Connection::disconnect(): finished." << std::endl;
#endif
		}

		~Connection()
		{
#ifdef _DEBUG
			_out << "Connection::~Connection()" << std::endl;
#endif
			disconnect();
#ifdef _DEBUG
			_out << "Connection::~Connection(): finished." << std::endl;
#endif
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
#ifdef _DEBUG
					_out << "Connection::run(): connection done, waiting for reconnect or quit signal." << std::endl;
#endif
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
#ifdef _DEBUG
					_out << "Connection::run(): not first connection try. Sleep." << std::endl;
#endif
					std::this_thread::sleep_for(std::chrono::seconds(_sleepSeconds));
#ifdef _DEBUG
					_out << "Connection::run(): sleep done." << std::endl;
#endif
				}
				else
				{
#ifdef _DEBUG
					_out << "Connection::run(): first start, no sleep." << std::endl;
#endif
				}

				_connection.reset(new T);

				_out << "Connection::run(): trying to connect " << _host << ":" << _port << "." << std::endl;
				auto result = _connection->connect(_host, _port, std::bind(&Connection<T>::connectionStateChanged, this, std::placeholders::_1));
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
#ifdef _DEBUG
			_out << "Connection::run(): main cycle finished. Removing connection." << std::endl;
#endif
			if (_connection.get() != nullptr)
			{
#ifdef _DEBUG
				_out << "Connection::run(): found active connection. Stoping it." << std::endl;
#endif
				_connection->stop();
				_connection->wait();
#ifdef _DEBUG
				_out << "Connection::run(): active connection Stopped." << std::endl;
#endif
			}
			_connected = false;
			_connection.reset(nullptr);
#ifdef _DEBUG
			_out << "Connection::run(): finished." << std::endl;
#endif
		}

		void connectionStateChanged(int status)
		{
#ifdef _DEBUG
			_out << "Connection::connectionStateChanged(" << status << ")" << std::endl;
#endif
			switch (status)
			{
			case redox::Redox::CONNECTED :
#ifdef _DEBUG
				_out << "Connection::connectionStateChanged(" << status << "): connected." << std::endl;
#endif
				break;

			case redox::Redox::NOT_YET_CONNECTED :
			case redox::Redox::DISCONNECTED :
			case redox::Redox::CONNECT_ERROR :
			case redox::Redox::DISCONNECT_ERROR :
			case redox::Redox::INIT_ERROR :
			default :
#ifdef _DEBUG
				_out << "Connection::connectionStateChanged(" << status << "): connection error." << std::endl;
#endif
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

	std::ostream& _out;
	Connection<redox::Redox> _redox;
	std::mutex _mutex;
	bool _quit;
	size_t _sentDeals;
	size_t _completeDeals;
	size_t _sentUserInfoRequests;
	size_t _completeUserInfoRequests;
	std::condition_variable _quitCV;
	std::stack<Deal> _deals;
};

#endif
