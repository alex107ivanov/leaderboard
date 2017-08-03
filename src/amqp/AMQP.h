#if defined(_MSC_VER) && _MSC_VER >= 1400 
#pragma warning(push) 
#pragma warning(disable:4996) 
#endif 

#include <vector>
#include <string>
#include <iostream>

#include <amqp.h>

#include <boost/thread.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/signals2.hpp>
#include <boost/circular_buffer.hpp>

class AMQP : boost::noncopyable
{
public:
	AMQP(std::iostream& _out, const std::string& host, int port, const std::string& login, const std::string& password, const std::string& vhost);

	~AMQP();

	void addExchange(const std::string& name, const std::string& type, const std::string& routingKey = "", bool listen = true);

	void addQueue(const std::string& name, bool persistent = false);

	void send(const std::string& exchange, const std::string& queue, const std::string& data, bool persistent = false);

	void quit();

	void join();

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

	std::iostream& _out;
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
