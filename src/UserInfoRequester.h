#ifndef USERINFOREQUESTER_H_DEFINED
#define USERINFOREQUESTER_H_DEFINED

#include <iostream>
#include <cstddef>
#include <thread>
#include <mutex>

#include <boost/noncopyable.hpp>
#include <boost/signals2.hpp>

class UserInfoRequester : private boost::noncopyable
{
public:
	UserInfoRequester(std::ostream& out, size_t intervalSec);

	void enableUser(size_t userid);

	void disableUser(size_t userid);

	void quit();

	void join();

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
