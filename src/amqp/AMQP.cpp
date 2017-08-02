#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <stdint.h>
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>

#include <assert.h>

#include "../../include/logstream/Logstream.h"

#include "../../include/exchange/AMQP.h"

AMQP::AMQP(Logstream& out, const std::string& host, int port, const std::string& login, const std::string& password, const std::string& vhost) :
	_out(out), _host(host), _port(port), _login(login), _password(password), _vhost(vhost), _quit(false), _connected(false)
{
	_thread.reset(new boost::thread(boost::bind(&AMQP::run, this)));
}

AMQP::~AMQP()
{
	_out << "[AMQP] destructor." << std::endl;

	{
		boost::mutex::scoped_lock lock(_mutex);

		_quit = true;
	}

	boost::this_thread::sleep(boost::posix_time::milliseconds(500));

	{
		boost::mutex::scoped_lock lock(_mutex);

		disconnect();

		_out << "[AMQP] destructor waiting for thread." << std::endl;
	}

	_thread->join();

	_out << "[AMQP] destructor finished." << std::endl;
}

void AMQP::disconnect()
{
//	boost::mutex::scoped_lock lock(_mutex);

	_out << "[AMQP] disconnect." << std::endl;

	if (!_connected)
	{
		_out << "[AMQP] not connected." << std::endl;

		return;
	}

	showError(amqp_channel_close(_connection, 1, AMQP_REPLY_SUCCESS));
	showError(amqp_connection_close(_connection, AMQP_REPLY_SUCCESS));
	showError(amqp_destroy_connection(_connection));

	_connected = false;

	_out << "[AMQP] disconnect done." << std::endl;

	return;
}

void AMQP::addQueue(const std::string& name, bool ack)
{
	if (!connect())
		return;

	boost::mutex::scoped_lock lock(_mutex);

	Queue queue = {name, ack};

	_queues.push_back(queue);

	loadQueue(queue);

	return;
}

void AMQP::loadQueue(const Queue& queue)
{
	amqp_basic_consume(_connection, 1, to_amqp_bytes(queue.name), amqp_empty_bytes, 0, (queue.ack ? 0 : 1), 0, amqp_empty_table);

	if (!showError(amqp_get_rpc_reply(_connection)))
	{
		_out << "[AMQP] Error while amqp_basic_consume('" << queue.name << "')" << std::endl;
		return;
	}

	_out << "[AMQP] Queue '" << queue.name << "' added." << std::endl;

	return;

}

void AMQP::addExchange(const std::string& name, const std::string& type, const std::string& routingKey, bool listen)
{
	if (!connect())
		return;

	boost::mutex::scoped_lock lock(_mutex);

	Exchange exchange = {name, type, routingKey, listen};

	_exchanges.push_back(exchange);

	loadExchange(exchange);

	return;
}

void AMQP::loadExchange(const Exchange& exchange)
{
	amqp_exchange_declare(_connection, 1, to_amqp_bytes(exchange.name), to_amqp_bytes(exchange.type),
				0, 0, 0, 0, amqp_empty_table);

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
	_out << "[AMQP] Storing message of " << data.size() << " bytes to '" << exchange << "'/'" << routingKey << "' to output queue." << std::endl;

	boost::mutex::scoped_lock lock(_mutex);

	_inputBuffer.push_back({exchange, routingKey, data, persistent});

	_conditionVariable.notify_all();

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

void AMQP::reconnect()
{
	boost::mutex::scoped_lock lock(_mutex);

	disconnect();

	connect();

	return;
}

bool AMQP::connect()
{
	boost::mutex::scoped_lock lock(_mutex);

	if (_quit)
		return false;

	if (_connected)
		return true;

	int tryCount = 0;

	while (true)
	{
		if (tryCount > 0)
		{
			_out << "[AMQP] Not first try. Sleeping..." << std::endl;

			boost::this_thread::sleep(boost::posix_time::seconds(5));
		}

		++tryCount;

		_out << "[AMQP] [" << tryCount << "] Trying to connect..." << std::endl;

		int status;
		amqp_socket_t *socket = NULL;

		_connection = amqp_new_connection();

		socket = amqp_tcp_socket_new(_connection);
		if (!socket)
		{
			_out << "[AMQP] Error creating socket." << std::endl;
			continue;
		}

		_out << "[AMQP] TCP socket created." << std::endl;

		status = amqp_socket_open(socket, _host.c_str(), _port);
		if (status)
		{
			_out << "[AMQP] Error connecting socket." << std::endl;
			continue;
		}

		_out << "[AMQP] TCP socket connected." << std::endl;

		if (!showError(amqp_login(_connection, _vhost.c_str(), 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, _login.c_str(), _password.c_str())))
		{
			_out << "[AMQP] Error while login." << std::endl;
			continue;
		}

		_out << "[AMQP] Login done." << std::endl;

		amqp_channel_open(_connection, 1);

		if (!showError(amqp_get_rpc_reply(_connection)))
		{
			_out << "[AMQP] Error while opening channel." << std::endl;
			continue;
		}

		_out << "[AMQP] Channel opened." << std::endl;

		amqp_basic_consume(_connection, 1, amqp_cstring_bytes("amq.rabbitmq.reply-to"), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
		if (!showError(amqp_get_rpc_reply(_connection)))
		{
			_out << "[AMQP] Error on amqp_basic_consume(amq.rabbitmq.reply-to)" << std::endl;
			continue;
		}

		_out << "[AMQP] Reply-to queue created." << std::endl;

		{
			amqp_queue_declare_ok_t *r = amqp_queue_declare(_connection, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
			if (!showError(amqp_get_rpc_reply(_connection)))
			{
				_out << "[AMQP] Error while creating direct queue." << std::endl;
				continue;
			}

			_directQueue = std::string((const char*)r->queue.bytes, r->queue.len);

			_out << "[AMQP] We created personal queue '" << _directQueue << "'" << std::endl;
		}

		amqp_basic_consume(_connection, 1, to_amqp_bytes(_directQueue), amqp_empty_bytes, 0, 0, 0, amqp_empty_table);
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
		timeout.tv_usec = 100000;

		res = amqp_consume_message(_connection, &envelope, &timeout, 0);

		if (AMQP_RESPONSE_NORMAL == res.reply_type)
		{
			_out << "[AMQP] Got incomming message." << std::endl;

			Message message = parseEnvelope(envelope);

			// ACK
			if(!showError(amqp_basic_ack(_connection, 1, envelope.delivery_tag, 0)))
			{
				_out << "[AMQP] Error while amqp_basic_ack." << std::endl;
			}

			amqp_destroy_envelope(&envelope);

			lock.unlock();

			onMessage(message.exchange, message.routingKey, message.contentType, message.replyTo, message.data);

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

		boost::this_thread::sleep(boost::posix_time::milliseconds(50));
	}

	_out << "[AMQP] inputRun finished." << std::endl;

	return;
}

AMQP::Message AMQP::parseEnvelope(const amqp_envelope_t& envelope)
{
	Message message;

	unsigned int deliveryTag = (unsigned int)envelope.delivery_tag;
	std::string exchange((const char*)envelope.exchange.bytes, envelope.exchange.len);
	std::string routingKey((const char*)envelope.routing_key.bytes, envelope.routing_key.len);
	std::string contentType;
	std::string replyTo;
	std::string data((const char*)envelope.message.body.bytes, envelope.message.body.len);

	_out << "[AMQP] Delivery " << deliveryTag << " '" << exchange << "' / '" << routingKey << "'" << std::endl;

	if (envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG)
	{
		contentType = std::string((const char*)envelope.message.properties.content_type.bytes, envelope.message.properties.content_type.len);
		_out << "[AMQP] Content-type: " << contentType << std::endl;
	}

	if (envelope.message.properties._flags & AMQP_BASIC_REPLY_TO_FLAG)
	{
		replyTo = std::string((const char*)envelope.message.properties.reply_to.bytes, envelope.message.properties.reply_to.len);
		_out << "[AMQP] Reply-to: " << replyTo << std::endl;
	}

	_out << "[AMQP] Message size is " << data.size() << std::endl;

	message = {exchange, routingKey, contentType, replyTo, data};

	return message;
}

void AMQP::outputRun()
{
	while (!_quit)
	{
		boost::mutex::scoped_lock lock(_mutex);

		while (_outputBuffer.empty())
			_conditionVariable.wait(lock);

		lock.unlock();

		if (!connect())
		{
			boost::this_thread::sleep(boost::posix_time::seconds(1));
			continue;
		}

		while (true)
		{
			lock.lock();

			if (_inputBuffer.empty())
				break;

			const auto& message = _outputBuffer.front();
			const auto& data = message.data;
			const auto& exchange = message.exchange;
			const auto& routingKey = message.routingKey;
			const auto& persistent = message.persistent;
			_out << "[AMQP] Sending message of " << data.size() << " bytes to '" << exchange << "'/'" << routingKey << "'" << std::endl;

			amqp_basic_properties_t props;

			props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
			//props.content_type = amqp_cstring_bytes("text/plain");
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
			}

			_outputBuffer.pop_front();

			lock.unlock();
		}
	}

	_out << "[AMQP] outputRun finished." << std::endl;

	return;
}


void AMQP::quit()
{
	_quit = true;

	_thread->join();

	return;
}

void AMQP::join()
{
	_thread->join();
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
