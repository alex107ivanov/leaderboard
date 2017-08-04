#include <vector>
#include <string>
#include <iostream>

#include <boost/thread.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/signals2.hpp>
#include <boost/circular_buffer.hpp>

#include <amqp.h>

/// Подключение к серверу AMQP.
class AMQP : private boost::noncopyable
{
public:
	/// Конструктор.
	/// @param out Поток для вывода сообщений.
	/// @param host Имя сервера для подключения.
	/// @param port Порт для подключения.
	/// @param login Имя пользователя.
	/// @param password Пароль.
	/// @param vhost Виртуальный хост.
	AMQP(std::ostream& out, const std::string& host, int port, const std::string& login, const std::string& password, const std::string& vhost);

	/// Деструктор.
	~AMQP();

	/// Добавляет точку обмена.
	/// @param name Имя.
	/// @param type Тип.
	/// @param routingKey Ключ маршрутизации.
	/// @param listen Признак необходимости получать сообщения.
	void addExchange(const std::string& name, const std::string& type, const std::string& routingKey = "", bool listen = true);

	/// Добавляет очередь.
	/// @param name Имя.
	/// @param persistent Признак persistent.
	void addQueue(const std::string& name, bool persistent = false);

	/// Отправляет сообщение.
	/// @param exchange Точка обмена.
	/// @param queue Очередь.
	/// @param data Данные.
	/// @param persistent Признак persistent.
	void send(const std::string& exchange, const std::string& queue, const std::string& data, bool persistent = false);

	/// Инициирует завершение работы.
	void quit();

	/// Возвращает управление по завершении работы.
	void join();

	/// Сигнал о поступлении сообщения.
	/// @param exchange Точка обмена.
	/// @param routingKey Ключь маршрутизации.
	/// @param contentType Тип содержимого.
	/// @param replyTo Адрес для ответа.
	/// @param data Данные.
	boost::signals2::signal<void (const std::string& /*exchange*/, const std::string& /*routingKey*/, const std::string& /*contentType*/, const std::string& /*replyTo*/, const std::string& /*data*/)> onMessage;

private:
	struct Exchange
	{
		std::string name;
		std::string type;
		std::string routingKey;
		bool listen;
	};

	struct Queue
	{
		std::string name;
		bool ack;
	};

	struct Message
	{
		std::string exchange;
		std::string routingKey;
		std::string contentType;
		std::string replyTo;
		std::string data;
	};

	struct OutputMessage
	{
		std::string exchange;
		std::string queue;
		std::string data;
		bool persistent;
	};

	template<typename T>
	inline amqp_bytes_t to_amqp_bytes(const T& data, bool copy = false)
	{
		amqp_bytes_t result;
		result.bytes = const_cast<T*>(&data);
		result.len = sizeof data;
		return to_amqp_bytes(result, copy);
	}

	void loadQueue(const Queue& queue);

	void loadExchange(const Exchange& exchange);

	bool showError(amqp_rpc_reply_t x);

	bool showError(int x);

	Message parseEnvelope(const amqp_envelope_t& envelope);

	bool connect();

	void disconnect();

	void inputRun();
	void outputRun();

	std::ostream& _out;
	std::string _host;
	int _port;
	std::string _login;
	std::string _password;
	std::string _vhost;

	bool _quit;
	boost::scoped_ptr<boost::thread> _inputThread;
	boost::scoped_ptr<boost::thread> _outputThread;
	boost::mutex _mutex;
	boost::condition_variable _conditionVariable;

	amqp_connection_state_t _connection;
	bool _connected;
	std::vector<Queue> _queues;
	std::vector<Exchange> _exchanges;
	std::string _directQueue;

	boost::circular_buffer<OutputMessage> _outputBuffer;
};

template<> inline
amqp_bytes_t AMQP::to_amqp_bytes<amqp_bytes_t>(const amqp_bytes_t& bytes, bool copy);

template<> inline
amqp_bytes_t AMQP::to_amqp_bytes<std::string>(const std::string& str, bool copy);
