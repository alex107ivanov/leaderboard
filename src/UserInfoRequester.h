#ifndef USERINFOREQUESTER_H_DEFINED
#define USERINFOREQUESTER_H_DEFINED

#include <iostream>
#include <cstddef>
#include <thread>
#include <mutex>

#include <boost/noncopyable.hpp>
#include <boost/signals2.hpp>

/// Формирует запросы информации о пользователях.
class UserInfoRequester : private boost::noncopyable
{
public:
	/// Конструктор.
	/// @param out Поток для вывода сообщений.
	/// @param intervalSec Интервал периодического формирования запросов по всем активным пользователям, сек.
	UserInfoRequester(std::ostream& out, size_t intervalSec);

	/// Активирует формирование запросов для заданного пользователя.
	void enableUser(size_t userid);

	/// Деактивирует формирование запросов для заданного пользователя.
	void disableUser(size_t userid);

	/// Инициирует завершение работы.
	void quit();

	/// Возвращает управление по завершении работы.
	void join();

	/// Сигнал о необходимости предоставить данные по заданному пользователю.
	boost::signals2::signal<void (size_t userid)> onRequest;

private:
	void run();

	std::ostream& _out;
	size_t _intervalSec;
	bool _quit;
	std::thread _thread;
	std::mutex _mutex;
	std::map<size_t, size_t> _users;
};

#endif
