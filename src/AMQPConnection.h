#ifndef AMQPCONNECTION_H_DEFINED
#define AMQPCONNECTION_H_DEFINED

#include <iostream>
#include <string>
#include <cstdint>

#include <boost/noncopyable.hpp>

#include <google/protobuf/message.h>

#include "AMQP.h"
#include "UserInfo.h"

/// Подключение к серверу AMQP.
class AMQPConnection : private boost::noncopyable
{
public:
	/// Конструктор.
	/// @param out Поток для вывода сообщений.
	/// @param host Адрес сервера.
	/// @param port Порт сервера.
	/// @param login Имя пользователя.
	/// @param password Пароль.
	/// @param vhost Виртуальный хост.
	AMQPConnection(std::ostream& out, const std::string& host, int port,
		const std::string& login, const std::string& password,
		const std::string& vhost);

	/// Деструктор.
	~AMQPConnection();

	/// Возвращает управление по завершении работы.
	void join();

	/// Инициирует завершение работы.
	void quit();

	/// Отправляет данные о пользователе.
	void sendUserInfo(const UserInfo& userInfo);

	/// Сигнал о поступлении сообщения о регистрации пользователя.
	boost::signals2::signal<void (size_t userid, const std::string& name)> onUserRegistered;
	/// Сигнал о поступлении сообщения о смене имени пользователя.
	boost::signals2::signal<void (size_t userid, const std::string& name)> onUserRenamed;
	/// Сигнал о поступлении сообщения о сделке пользователя.
	boost::signals2::signal<void (size_t userid, time_t time, float amount)> onUserDeal;
	/// Сигнал о поступлении сообщения о прибыльной сделке пользователя.
	boost::signals2::signal<void (size_t userid, time_t time, float amount)> onUserDealWon;
	/// Сигнал о поступлении сообщения о подключении пользователя.
	boost::signals2::signal<void (size_t userid)> onUserConnected;
	/// Сигнал о поступлении сообщения об отключении пользователя.
	boost::signals2::signal<void (size_t userid)> onUserDisconnected;

private:
	void handleMessage(const std::string& exchange, const std::string& routingKey, const std::string& contentType, const std::string& replyTo, const std::string& data);

	std::ostream& _out;

	AMQP _amqp;
};

#endif
