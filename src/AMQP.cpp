#include "AMQP.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <stdint.h>
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>

#include <assert.h>
#include <chrono>

AMQP::AMQP(std::ostream& out, const std::string& host, int port, const std::string& login, const std::string& password, const std::string& vhost) :
	_out(out), _host(host), _port(port), _login(login), _password(password), _vhost(vhost), _quit(false), _connected(false)
{
	_outputBuffer.set_capacity(512);
	_inputThread.reset(new boost::thread(boost::bind(&AMQP::inputRun, this)));
	_outputThread.reset(new boost::thread(boost::bind(&AMQP::outputRun, this)));
}

AMQP::~AMQP()
{
#ifdef _DEBUG
	_out << "[AMQP] destructor." << std::endl;
#endif

	{
		boost::mutex::scoped_lock lock(_mutex);

		_quit = true;
	}

	boost::this_thread::sleep(boost::posix_time::milliseconds(500));

	{
		boost::mutex::scoped_lock lock(_mutex);

		disconnect();

#ifdef _DEBUG
		_out << "[AMQP] destructor waiting for thread." << std::endl;
#endif
	}

	_conditionVariable.notify_all();

	_inputThread->join();
	_outputThread->join();

#ifdef _DEBUG
	_out << "[AMQP] destructor finished." << std::endl;
#endif
}

void AMQP::disconnect()
{
#ifdef _DEBUG
	_out << "[AMQP] disconnect." << std::endl;
#endif

	if (!_connected)
	{
#ifdef _DEBUG
		_out << "[AMQP] not connected." << std::endl;
#endif
		return;
	}

	showError(amqp_channel_close(_connection, 1, AMQP_REPLY_SUCCESS));
	showError(amqp_connection_close(_connection, AMQP_REPLY_SUCCESS));
	showError(amqp_destroy_connection(_connection));

	_connected = false;

#ifdef _DEBUG
	_out << "[AMQP] disconnect done." << std::endl;
#endif
	return;
}

void AMQP::addQueue(const std::string& name, bool ack)
{
	boost::mutex::scoped_lock lock(_mutex);

	Queue queue = {name, ack};

	_queues.push_back(queue);

	if (_connected)
		loadQueue(queue);

	return;
}

void AMQP::loadQueue(const Queue& queue)
{
	amqp_basic_consume(_connection, 1, to_amqp_bytes(queue.name), amqp_empty_bytes, 0, (queue.ack ? 0 : 1), 0, amqp_empty_table);
	/*
	amqp_connection_state_t state, 
	amqp_channel_t channel, 
	amqp_bytes_t queue, 
	amqp_bytes_t consumer_tag, 
	amqp_boolean_t no_local, 
	amqp_boolean_t no_ack, 
	amqp_boolean_t exclusive, 
	amqp_table_t arguments
	*/

	if (!showError(amqp_get_rpc_reply(_connection)))
	{
		_out << "[AMQP] Error while amqp_basic_consume('" << queue.name << "')" << std::endl;
		return;
	}

#ifdef _DEBUG
	_out << "[AMQP] Queue '" << queue.name << "' added." << std::endl;
#endif

	return;

}

void AMQP::addExchange(const std::string& name, const std::string& type, const std::string& routingKey, bool listen)
{
	boost::mutex::scoped_lock lock(_mutex);

	Exchange exchange = {name, type, routingKey, listen};

	_exchanges.push_back(exchange);

	if (_connected)
		loadExchange(exchange);

	return;
}

void AMQP::loadExchange(const Exchange& exchange)
{
	amqp_exchange_declare(_connection, 1, to_amqp_bytes(exchange.name), to_amqp_bytes(exchange.type),
				0, 0, 0, 0, amqp_empty_table);

//	amqp_exchange_declare(_connection, 1, to_amqp_bytes(exchange.name), to_amqp_bytes(exchange.type),
//				0, 0, amqp_empty_table);

/*
amqp_connection_state_t state, 
amqp_channel_t channel, 
amqp_bytes_t exchange, 
amqp_bytes_t type, 
amqp_boolean_t passive, 
amqp_boolean_t durable, 
amqp_table_t arguments
*/
	showError(amqp_get_rpc_reply(_connection));

	if (!exchange.listen)
		return;

	amqp_queue_bind(_connection, 1,
			to_amqp_bytes(_directQueue),
			to_amqp_bytes(exchange.name),
//			to_amqp_bytes(exchange.routingKey.c_str()),
			to_amqp_bytes(_directQueue),
			amqp_empty_table);

	if (!showError(amqp_get_rpc_reply(_connection)))
	{
		_out << "[AMQP] Error on amqp_queue_bind '" << _directQueue << "' to '" << exchange.name << "'" << std::endl;
	}

	return;

}

void AMQP::send(const std::string& exchange, const std::string& routingKey, const std::string& data, bool persistent)
{
#ifdef _DEBUG
	_out << "[AMQP] Storing message of " << data.size() << " bytes to '" << exchange << "'/'" << routingKey << "' to output queue." << std::endl;
#endif

	const auto& beginTime = std::chrono::high_resolution_clock::now();

	while(true)
	{
		bool full = false;
		{
			boost::mutex::scoped_lock lock(_mutex);
#ifdef _DEBUG
			_out << "[AMQP] Output buffer contains " << _outputBuffer.size() << " elements." << std::endl;
#endif
			full = _outputBuffer.full();
		}

		if (full)
		{
#ifdef _DEBUG
			_out << "[AMQP] Output buffer is full!" << std::endl;
#endif
			boost::this_thread::sleep(boost::posix_time::milliseconds(10));
		}
		else
		{
			break;
		}
	}

	{
		boost::mutex::scoped_lock lock(_mutex);

		_outputBuffer.push_back({exchange, routingKey, data, persistent});

		_conditionVariable.notify_all();
	}

	const auto& endTime = std::chrono::high_resolution_clock::now();
	const auto& msec = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime).count();
	if (msec > 100)
	{
		_out << "[AMQP] message stored in " << msec << " msec." << std::endl;
	}

	return;
}

bool AMQP::showError(int x)
{
	if (x < 0)
	{
		_out << "[AMQP] "<< amqp_error_string2(x) << std::endl;
		return false;
	}
	return true;
}

bool AMQP::showError(amqp_rpc_reply_t x)
{
	switch (x.reply_type)
	{
	case AMQP_RESPONSE_NORMAL :
		return true;

	case AMQP_RESPONSE_NONE :
		_out << "[AMQP] missing RPC reply type!" << std::endl;
		break;

	case AMQP_RESPONSE_LIBRARY_EXCEPTION :
		_out << "[AMQP] " << amqp_error_string2(x.library_error) << std::endl;
		break;

	case AMQP_RESPONSE_SERVER_EXCEPTION :
		switch (x.reply.id)
		{
		case AMQP_CONNECTION_CLOSE_METHOD :
			{
				amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
				std::string replyText((const char*)m->reply_text.bytes, m->reply_text.len);
				_out << "[AMQP] server connection error " << m->reply_code << ", message: " << replyText << std::endl;
				break;
			}
		case AMQP_CHANNEL_CLOSE_METHOD :
			{
				amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
				std::string replyText((const char*)m->reply_text.bytes, m->reply_text.len);
				_out << "[AMQP] server channel error " << m->reply_code << ", message: " << replyText << std::endl;
				break;
			}
		default :
			_out << "[AMQP] unknown server error, method id " << x.reply.id << std::endl;
			break;
		}
		break;
	}
	return false;
}

bool AMQP::connect()
{
	boost::mutex::scoped_lock lock(_mutex);

	int tryCount = 0;

	while (true)
	{
		if (tryCount > 0)
		{
			_out << "[AMQP] Not first try. Sleeping..." << std::endl;
			lock.unlock();
			boost::this_thread::sleep(boost::posix_time::seconds(5));
			lock.lock();
		}

		if (_quit)
			return false;

		if (_connected)
			return true;

		++tryCount;

		_out << "[AMQP] [" << tryCount << "] Trying to connect to " << _host << " : " << _port << "..." << std::endl;

		int status;
		amqp_socket_t* socket = nullptr;

		_connection = amqp_new_connection();

		socket = amqp_tcp_socket_new(_connection);
		if (!socket)
		{
			_out << "[AMQP] Error creating socket." << std::endl;
			continue;
		}

#ifdef _DEBUG
		_out << "[AMQP] TCP socket created." << std::endl;
#endif

		struct timeval timeout;
		timeout.tv_sec = 5;
		timeout.tv_usec = 0;
		status = amqp_socket_open_noblock(socket, _host.c_str(), _port, &timeout);
		if (status)
		{
			_out << "[AMQP] Error connecting socket." << std::endl;
			continue;
		}

#ifdef _DEBUG
		_out << "[AMQP] TCP socket connected." << std::endl;
#endif

		if (!showError(amqp_login(_connection, _vhost.c_str(), 0, 131072/*frame max*/, 60/*heartbeat*/, AMQP_SASL_METHOD_PLAIN, _login.c_str(), _password.c_str())))
		{
			_out << "[AMQP] Error while login." << std::endl;
			continue;
		}

#ifdef _DEBUG
		_out << "[AMQP] Login done." << std::endl;
#endif

		amqp_channel_open(_connection, 1);

		if (!showError(amqp_get_rpc_reply(_connection)))
		{
			_out << "[AMQP] Error while opening channel." << std::endl;
			continue;
		}

#ifdef _DEBUG
		_out << "[AMQP] Channel opened." << std::endl;
#endif

		amqp_basic_consume(_connection, 1, amqp_cstring_bytes("amq.rabbitmq.reply-to"), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
		/*
		amqp_connection_state_t state, 
		amqp_channel_t channel, 
		amqp_bytes_t queue, 
		amqp_bytes_t consumer_tag, 
		amqp_boolean_t no_local, 
		amqp_boolean_t no_ack, 
		amqp_boolean_t exclusive, 
		amqp_table_t arguments
		*/
		if (!showError(amqp_get_rpc_reply(_connection)))
		{
			_out << "[AMQP] Error on amqp_basic_consume(amq.rabbitmq.reply-to)" << std::endl;
			continue;
		}

#ifdef _DEBUG
		_out << "[AMQP] Reply-to queue created." << std::endl;
#endif

		{
			amqp_queue_declare_ok_t *r = amqp_queue_declare(_connection, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
			/*
			amqp_connection_state_t		state,
			amqp_channel_t 	channel,
			amqp_bytes_t 	queue,
			amqp_boolean_t 	passive,
			amqp_boolean_t 	durable,
			amqp_boolean_t 	exclusive,
			amqp_boolean_t 	auto_delete,
			amqp_table_t 	arguments
			*/
			if (!showError(amqp_get_rpc_reply(_connection)))
			{
				_out << "[AMQP] Error while creating direct queue." << std::endl;
				continue;
			}

			_directQueue = std::string((const char*)r->queue.bytes, r->queue.len);

#ifdef _DEBUG
			_out << "[AMQP] We created personal queue '" << _directQueue << "'" << std::endl;
#endif
		}

		amqp_basic_consume(_connection, 1, to_amqp_bytes(_directQueue), amqp_empty_bytes, 0, 0, 0, amqp_empty_table);
		/*
		amqp_connection_state_t state, 
		amqp_channel_t channel, 
		amqp_bytes_t queue, 
		amqp_bytes_t consumer_tag, 
		amqp_boolean_t no_local, 
		amqp_boolean_t no_ack, 
		amqp_boolean_t exclusive, 
		amqp_table_t arguments
		*/
		if (!showError(amqp_get_rpc_reply(_connection)))
		{
			_out << "[AMQP] Error on amqp_basic_consume('" << _directQueue << "')" << std::endl;
			continue;
		}

		// loading saved queues & exchanged
		for (const auto& queue : _queues)
			loadQueue(queue);

		for (const auto& exchange : _exchanges)
			loadExchange(exchange);

		_connected = true;

		_out << "[AMQP] Connection to server OK." << std::endl;

		return true;
	}
}

void AMQP::inputRun()
{
	while (!_quit)
	{
		if (!connect())
			continue;

		amqp_rpc_reply_t res;
		amqp_envelope_t envelope;

		boost::mutex::scoped_lock lock(_mutex);

		amqp_maybe_release_buffers(_connection);

		struct timeval timeout;
		timeout.tv_sec = 0;
		timeout.tv_usec = 10000; // 0.01 sec

		res = amqp_consume_message(_connection, &envelope, &timeout, 0);

		if (AMQP_RESPONSE_NORMAL == res.reply_type)
		{
#ifdef _DEBUG
			_out << "[AMQP] Got incomming message." << std::endl;
#endif

			Message message = parseEnvelope(envelope);

			// ACK
			if(!showError(amqp_basic_ack(_connection, 1, envelope.delivery_tag, 0)))
			{
				_out << "[AMQP] Error while amqp_basic_ack." << std::endl;
				disconnect();
			}
			else
			{
				lock.unlock();
				onMessage(message.exchange, message.routingKey, message.contentType, message.replyTo, message.data);
			}

			amqp_destroy_envelope(&envelope);

			continue;
		}
		else if (AMQP_RESPONSE_LIBRARY_EXCEPTION == res.reply_type && res.library_error == AMQP_STATUS_TIMEOUT)
		{
			// timeout;
		}
		else
		{
			disconnect();

			_out << "[AMQP] res.reply_type = " << res.reply_type << ", res.library_error = " << res.library_error << std::endl;

			continue;
		}
		lock.unlock();

		boost::this_thread::sleep(boost::posix_time::milliseconds(1));
	}

#ifdef _DEBUG
	_out << "[AMQP] inputRun finished." << std::endl;
#endif

	return;
}

AMQP::Message AMQP::parseEnvelope(const amqp_envelope_t& envelope)
{
	Message message;

#ifdef _DEBUG
	unsigned int deliveryTag = (unsigned int)envelope.delivery_tag;
#endif
	std::string exchange((const char*)envelope.exchange.bytes, envelope.exchange.len);
	std::string routingKey((const char*)envelope.routing_key.bytes, envelope.routing_key.len);
	std::string contentType;
	std::string replyTo;
	std::string data((const char*)envelope.message.body.bytes, envelope.message.body.len);

#ifdef _DEBUG
	_out << "[AMQP] Delivery " << deliveryTag << " '" << exchange << "' / '" << routingKey << "'" << std::endl;
#endif

	if (envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG)
	{
		contentType = std::string((const char*)envelope.message.properties.content_type.bytes, envelope.message.properties.content_type.len);
#ifdef _DEBUG
		_out << "[AMQP] Content-type: " << contentType << std::endl;
#endif
	}

	if (envelope.message.properties._flags & AMQP_BASIC_REPLY_TO_FLAG)
	{
		replyTo = std::string((const char*)envelope.message.properties.reply_to.bytes, envelope.message.properties.reply_to.len);
#ifdef _DEBUG
		_out << "[AMQP] Reply-to: " << replyTo << std::endl;
#endif
	}

#ifdef _DEBUG
	_out << "[AMQP] Message size is " << data.size() << std::endl;
#endif

	message = {exchange, routingKey, contentType, replyTo, data};

	return message;
}

void AMQP::outputRun()
{
	while (!_quit)
	{
		boost::this_thread::sleep(boost::posix_time::milliseconds(1));

		boost::mutex::scoped_lock lock(_mutex);

		while (!_quit && _outputBuffer.empty())
			_conditionVariable.wait(lock);

		if (_quit)
		{
#ifdef _DEBUG
			_out << "[AMQP] AMQP::outputRun(): got quit flag." << std::endl;
#endif
			break;
		}

#ifdef _DEBUG
		_out << "[AMQP] We got " << _outputBuffer.size() << " messages in output queue." << std::endl;
#endif

		lock.unlock();

		if (!connect())
		{
			boost::this_thread::sleep(boost::posix_time::seconds(1));
			continue;
		}

#ifdef _DEBUG
		const auto& beginTime = std::chrono::high_resolution_clock::now();
#endif
		size_t sentCount = 0;

		while (true)
		{
			lock.lock();

			if (_outputBuffer.empty())
			{
#ifdef _DEBUG
				_out << "[AMQP] Ouptup queue is empty." << std::endl;
#endif
				break;
			}

			const auto& message = _outputBuffer.front();
			const auto& data = message.data;
			const auto& exchange = message.exchange;
			const auto& routingKey = message.queue;
			const auto& persistent = message.persistent;
#ifdef _DEBUG
			_out << "[AMQP] Sending message [first of " <<_outputBuffer.size() << "] of " << data.size() << " bytes to '" << exchange << "'/'" << routingKey << "'" << std::endl;
#endif

			amqp_basic_properties_t props;

			props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
			props.content_type = amqp_cstring_bytes("application/octet-stream");

			if (persistent)
			{
				props._flags |= AMQP_BASIC_DELIVERY_MODE_FLAG;
				props.delivery_mode = 2; /* persistent delivery mode */
			}

			props._flags |= AMQP_BASIC_REPLY_TO_FLAG;
			props.reply_to = amqp_cstring_bytes("amq.rabbitmq.reply-to");

			if (!showError(amqp_basic_publish(_connection, 1, to_amqp_bytes(exchange), to_amqp_bytes(routingKey),
				0, 0, &props, to_amqp_bytes(data))))
			{
				_out << "[AMQP] Error on amqp_basic_publish" << std::endl;
				disconnect();
				break;
			}

			_outputBuffer.pop_front();

			lock.unlock();

			++sentCount;

#ifdef _DEBUG
			if (sentCount > 0 && sentCount % 500 == 0)
			{
				const auto& endTime = std::chrono::high_resolution_clock::now();
				const auto& msec = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime).count();
				const auto& speed = static_cast<float>(msec * 1000.0) / sentCount;
				_out << "[AMQP] " << sentCount << " messages sent, output speed is " << speed << " pkg/sec." << std::endl;
			}
#endif
		}

#ifdef _DEBUG
		if (sentCount > 100)
		{
			const auto& endTime = std::chrono::high_resolution_clock::now();
			const auto& msec = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - beginTime).count();
			const auto& speed = static_cast<float>(msec * 1000.0) / sentCount;
			_out << "[AMQP] " << sentCount << " messages sent, output speed is " << speed << " pkg/sec." << std::endl;
		}
#endif
	}

#ifdef _DEBUG
	_out << "[AMQP] outputRun finished." << std::endl;
#endif

	return;
}


void AMQP::quit()
{
	_quit = true;

	_conditionVariable.notify_all();

	_inputThread->join();

	_outputThread->join();

	return;
}

void AMQP::join()
{
	_inputThread->join();
	_outputThread->join();
}

template<> inline
amqp_bytes_t AMQP::to_amqp_bytes<amqp_bytes_t>(const amqp_bytes_t& bytes, bool copy)
{
	return copy ? amqp_bytes_malloc_dup(bytes) : bytes;
}

template<> inline
amqp_bytes_t AMQP::to_amqp_bytes<std::string>(const std::string& str, bool copy)
{
	amqp_bytes_t result;
	result.bytes = const_cast<char*>(str.data());
	result.len = str.size();
	return to_amqp_bytes(result, copy);
}
