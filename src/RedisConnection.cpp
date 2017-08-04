#include "RedisConnection.h"

#include <boost/lexical_cast.hpp>

#include "split.h"

RedisConnection::RedisConnection(std::ostream& out, const std::string& host, int port) :
	_out(out), 
	_redox(out, host, port, std::bind(&RedisConnection::redoxConnectedHandler, this)), 
	_quit(false),
	_sentDeals(0),
	_completeDeals(0),
	_sentUserInfoRequests(0),
	_completeUserInfoRequests(0)
{
#ifdef _DEBUG
	_out << "RedisConnection::RedisConnection()" << std::endl;
#endif
}

void RedisConnection::redoxConnectedHandler()
{
#ifdef _DEBUG
	_out << "RedisConnection::redoxConnectedHandler()" << std::endl;
#endif

	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
#ifdef _DEBUG
			_out << "RedisConnection::redoxConnectedHandler(): got quit." << std::endl;
#endif
			return;
		}
	}

	// send saved deals
	while (!_deals.empty())
	{
#ifdef _DEBUG
		_out << "RedisConnection::redoxConnectedHandler(): got " << _deals.size() << " deals in queue." << std::endl;
#endif
		const auto& deal = _deals.top();
		if (storeDeal(deal))
		{
			_deals.pop();
		}
	}

#ifdef _DEBUG
	_out << "RedisConnection::redoxConnectedHandler(): deal queue is empty." << std::endl;
#endif

	return;
}

RedisConnection::~RedisConnection()
{
#ifdef _DEBUG
	_out << "RedisConnection::~RedisConnection()" << std::endl;
#endif

	_redox.disconnect();

	quit();

	join();

	return;
}

void RedisConnection::quit()
{
#ifdef _DEBUG
	_out << "RedisConnection::quit()" << std::endl;
#endif

	{
		std::unique_lock<std::mutex> lock(_mutex);
		_quit = true;
	}

	_quitCV.notify_one();

#ifdef _DEBUG
	_out << "RedisConnection::quit(): finish." << std::endl;
#endif
}

void RedisConnection::join()
{
#ifdef _DEBUG
	_out << "RedisConnection::join()" << std::endl;
#endif

	std::unique_lock<std::mutex> lock(_mutex);
	while (!_quit)
		_quitCV.wait(lock);

#ifdef _DEBUG
	_out << "RedisConnection::join(): finish." << std::endl;
#endif
}

void RedisConnection::storeDeal(size_t userid, float amount, size_t doy)
{
#ifdef _DEBUG
	_out << "RedisConnection::storeDeal(size_t userid, float amount, size_t doy)" << std::endl;
#endif
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
#ifdef _DEBUG
			_out << "RedisConnection::storeDeal(size_t userid, float amount, size_t doy): got quit." << std::endl;
#endif
			return;
		}
	}

	if (!storeDeal({userid, amount, doy}))
	{
#ifdef _DEBUG
		_out << "RedisConnection::storeDeal(size_t userid, float amount, size_t doy): not connecting, saving deal for future." << std::endl;
#endif
		std::unique_lock<std::mutex> lock(_mutex);
		_deals.push({userid, amount, doy});
	}

	return;
}

bool RedisConnection::storeDeal(const Deal& deal)
{
#ifdef _DEBUG
	_out << "RedisConnection::storeDeal(Deal deal)" << std::endl;
#endif
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
#ifdef _DEBUG
			_out << "RedisConnection::storeDeal(Deal deal): got quit." << std::endl;
#endif
			return false;
		}
	}

	std::string strUserid = std::to_string(deal.userid);
	std::string strAmount = std::to_string(deal.amount);
	std::string strDoy = std::to_string(deal.doy);
	// TODO to constants
	std::string procedure =
"local userid = tonumber(ARGV[1]) "
"local score = tonumber(ARGV[2]) "
"local doy = tonumber(ARGV[3]) "
" "
"if not userid then "
"    return 'userid is empty' "
"end "
" "
"if not score then "
"    return 'score is empty' "
"end "
" "
"if not doy then "
"    return 'doy is empty' "
"end "
" "
"local current_doy = redis.call('HGET', 'user:' .. userid, 'current_doy') "
"if tonumber(current_doy) ~= doy then "
"  local histiry_len = redis.call('LLEN', 'score_day_history:' .. userid) "
"  if tonumber(histiry_len) >= 6 then "
"    local oldest_day_score = redis.call('LPOP', 'score_day_history:' .. userid) "
"    redis.call('ZINCRBY', 'scores', -1 * tonumber(oldest_day_score), userid) "
"  end "
"  local current_doy_score = redis.call('HGET', 'user:' .. userid, 'current_doy_score') "
"  if not tonumber(current_doy_score) then "
"    current_doy_score = 0 "
"  end "
"  redis.call('LPUSH', 'score_day_history:' .. userid, tonumber(current_doy_score)) "
"  redis.call('HSET', 'user:' .. userid, 'current_doy_score', 0) "
"  redis.call('HSET', 'user:' .. userid, 'current_doy', doy) "
"end "
" "
"local current_doy_score = redis.call('HGET', 'user:' .. userid, 'current_doy_score') "
"if not tonumber(current_doy_score) then "
"  current_doy_score = 0 "
"end "
"redis.call('HSET', 'user:' .. userid, 'current_doy_score', tonumber(current_doy_score) + score) "
"redis.call('ZINCRBY', 'scores', score, userid) "
" "
"return tonumber(current_doy_score) + score ";

	if (_redox.isConnected())
	{
		++_sentDeals;
		_redox->command<int>({"EVAL", procedure, "0", strUserid, strAmount, strDoy}, std::bind(&RedisConnection::storeDealReplyHandler, this, std::placeholders::_1));
		return true;
	}

	return false;
}

void RedisConnection::storeDealReplyHandler(redox::Command<int>& command)
{
#ifdef _DEBUG
	_out << "RedisConnection::storeDealReplyHandler(redox::Command<std::string>& command)" << std::endl;
#endif

	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
#ifdef _DEBUG
			_out << "RedisConnection::storeDealReplyHandler(redox::Command<std::string>& command): got quit." << std::endl;
#endif
			return;
		}
	}

	++_completeDeals;

	if(!command.ok())
	{
#ifdef _DEBUG
		_out << "Wrong answer on command." << std::endl;
#endif
		return;
	}

#ifdef _DEBUG
	_out << "Command complete. " << command.cmd() << ": " << command.reply() << std::endl;
#endif

	return;
}

bool RedisConnection::requestUserInfo(size_t userid)
{
#ifdef _DEBUG
	_out << "RedisConnection::requestUserInfo(size_t userid)" << std::endl;
#endif
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
#ifdef _DEBUG
			_out << "RedisConnection::requestUserInfo(size_t userid): got quit." << std::endl;
#endif
			return false;
		}
	}

	std::string strUserid = std::to_string(userid);
	// TODO to constants
	std::string procedure =
"local userid = tonumber(ARGV[1]) "
" "
"if not userid then "
"    return 'userid is empty' "
"end "
" "
"local score = redis.call('ZSCORE', 'scores', userid) "
" "
"if not score then "
"    return 'Userid ' .. userid .. ' not found.' "
"end "
" "
"local place = redis.call('ZREVRANK', 'scores', userid) "
"if not place then "
"    return 'Cant get place for user ' .. userid "
"end "
" "
"local start = tonumber(place) - 10 "
"if start < 0 then "
"    start = 0 "
"end "
" "
"local stop = tonumber(place) + 10 "
" "
"local result = redis.call('ZREVRANGE', 'scores', start, stop, 'withscores') "
" "
"local top = redis.call('ZREVRANGE', 'scores', 0, 10, 'withscores') "
" "
"local name = redis.call('HGET', 'user:' .. userid, 'name') "
" "
"if not name then "
"    name = '-' "
"end "
" "
"return {userid .. '', name .. '', score .. '', place .. '', table.concat(result, ','), table.concat(top, ',')}";

	if (_redox.isConnected())
	{
		++_sentUserInfoRequests;
		_redox->command<std::vector<std::string>>({"EVAL", procedure, "0", strUserid}, std::bind(&RedisConnection::requestUserInfoReplyHandler, this, std::placeholders::_1));
		return true;
	}

	return false;
}

void RedisConnection::requestUserInfoReplyHandler(redox::Command<std::vector<std::string>>& command)
{
#ifdef _DEBUG
	_out << "RedisConnection::requestUserInfoReplyHandler(redox::Command<std::string>& command)" << std::endl;
#endif

	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (_quit)
		{
#ifdef _DEBUG
			_out << "RedisConnection::requestUserInfoReplyHandler(redox::Command<std::string>& command): got quit." << std::endl;
#endif
			return;
		}
	}

	++_completeUserInfoRequests;

	if(!command.ok())
	{
		_out << "Wrong answer on command." << std::endl;
		return;
	}

#ifdef _DEBUG
	_out << "Command complete. " << command.cmd() << ": " << std::endl;
#endif

	UserInfo userInfo;

/*
	// userid
      DATA: 1000

	// name
      DATA: -

	// score
      DATA: 1234.4999699999996

	// place
      DATA: 0

	// around
      DATA: 1000,1234.4999699999996,23,124.5,22,124,17,112.17,11,112.09999999999999,5,36.600000000000009,13,17.149999999999999,20,14.5,19,13.5,12,12.15,10,12.1

	// top
      DATA: 1000,1234.4999699999996,23,124.5,22,124,17,112.17,11,112.09999999999999,5,36.600000000000009,13,17.149999999999999,20,14.5,19,13.5,12,12.15,10,12.1
*/

#ifdef _DEBUG
	for (const auto& line : command.reply())
	{
		std::cout << "      DATA: " << line << std::endl;
	}
#endif

	const auto& reply = command.reply();

	if (reply.size() != 6)
	{
		_out << "Reply size is wrong: " << reply.size() << std::endl;
		return;
	}

	try
	{
		userInfo.userid = boost::lexical_cast<size_t>(reply[0]);
#ifdef _DEBUG
		std::cout << " > userInfo.userid = " << userInfo.userid << std::endl;
#endif

		userInfo.name = reply[1];
#ifdef _DEBUG
		std::cout << " > userInfo.name = " << userInfo.name << std::endl;
#endif

		userInfo.amount = boost::lexical_cast<float>(reply[2]);
#ifdef _DEBUG
		std::cout << " > userInfo.amount = " << userInfo.amount << std::endl;
#endif

		userInfo.place = boost::lexical_cast<size_t>(reply[3]);
#ifdef _DEBUG
		std::cout << " > userInfo.place = " << userInfo.place << std::endl;
#endif

		auto aroundParts = split(reply[4], ',');
		if (aroundParts.size() % 2 != 0)
		{
			_out << "Wrong count of parts in around: " << aroundParts.size() << std::endl;
			return;
		}
		for (size_t i = 0; i < aroundParts.size(); i += 2)
		{
			size_t userid = boost::lexical_cast<size_t>(aroundParts[i]);
			float amount = boost::lexical_cast<float>(aroundParts[i + 1]);
			userInfo.around.push_back({userid, amount});
#ifdef _DEBUG
			std::cout << " > userInfo.around[" << userInfo.around.size() << "] = " << userInfo.around[userInfo.around.size() - 1].first << ", " << userInfo.around[userInfo.around.size() - 1].second << std::endl;
#endif
		}

		auto topParts = split(reply[5], ',');
		if (topParts.size() % 2 != 0)
		{
			_out << "Wrong count of parts in top: " << topParts.size() << std::endl;
			return;
		}

		for (size_t i = 0; i < topParts.size(); i += 2)
		{
			size_t userid = boost::lexical_cast<size_t>(topParts[i]);
			float amount = boost::lexical_cast<float>(topParts[i + 1]);
			userInfo.top.push_back({userid, amount});
#ifdef _DEBUG
			std::cout << " > userInfo.top[" << userInfo.top.size() << "] = " << userInfo.top[userInfo.top.size() - 1].first << ", " << userInfo.top[userInfo.top.size() - 1].second << std::endl;
#endif
		}
	}
	catch(const boost::bad_lexical_cast& error)
	{
		_out << "Erorr parsing reply: " << error.what() << std::endl;
		return;
	}

#ifdef _DEBUG
	std::cout << "userInfo.userid = " << userInfo.userid << std::endl;
	std::cout << "userInfo.name = " << userInfo.name << std::endl;
	std::cout << "userInfo.amount = " << userInfo.amount << std::endl;
	std::cout << "userInfo.place = " << userInfo.place << std::endl;
	std::cout << "userInfo.around:" << std::endl;
	for (const auto& around : userInfo.around)
		std::cout << "   - " << around.first << ", " << around.second << std::endl;
	std::cout << "userInfo.top:" << std::endl;
	for (const auto& top : userInfo.top)
		std::cout << "   - " << top.first << ", " << top.second << std::endl;
#endif
	onUserInfo(userInfo);
/*
        struct UserInfo
        {
                size_t userid;
                size_t place;
                float amount;
                std::vector<size_t> top;
                std::vector<size_t> around;
        };
*/

	return;
}

void RedisConnection::printStatistics()
{
	std::cout << "SentDeals = " << _sentDeals << std::endl;
	std::cout << "CompleteDeals = " << _completeDeals << std::endl;
	std::cout << "SentUserInfoRequests = " << _sentUserInfoRequests << std::endl;
	std::cout << "CompleteUserInfoRequests = " << _completeUserInfoRequests << std::endl;
	return;
}

bool RedisConnection::complete()
{
	return (_sentDeals == _completeDeals && _sentUserInfoRequests == _completeUserInfoRequests);
}
