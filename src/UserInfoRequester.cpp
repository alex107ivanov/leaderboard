#include "UserInfoRequester.h"

UserInfoRequester::UserInfoRequester(std::ostream& out, size_t intervalSec) :
	_out(out),
	_intervalSec(intervalSec),
	_quit(false),
	_thread(std::bind(&UserInfoRequester::run, this))
{
}

void UserInfoRequester::enableUser(size_t userid)
{
	std::unique_lock<std::mutex> lock(_mutex);
	_users[userid] = userid;
	onRequest(userid);
}

void UserInfoRequester::disableUser(size_t userid)
{
	std::unique_lock<std::mutex> lock(_mutex);
	auto it = _users.find(userid);
	if (it != _users.end())
		_users.erase(it);
}

void UserInfoRequester::quit()
{
	std::unique_lock<std::mutex> lock(_mutex);
	_quit = true;
}

void UserInfoRequester::join()
{
	_thread.join();
}

void UserInfoRequester::run()
{
	while (true)
	{
		{
			std::unique_lock<std::mutex> lock(_mutex);
			if (_quit)
				break;
		}
		//
		{
			// TODO remove this lock, it blocks enable/disable user => can block amqp receiver. copy of map?
			std::unique_lock<std::mutex> lock(_mutex);
#ifdef _DEBUG
			_out << "RUN" << std::endl;
#endif
			for (const auto& userid : _users)
			{
				onRequest(userid.first);
			}
		}
		//
		std::this_thread::sleep_for(std::chrono::seconds(_intervalSec));
	}
}
