#!/bin/sh

docker run -d \
	--name leaderboard-rabbitmq \
	-p 5672:5672 \
	-p 5673:5673 \
	-p 15672:15672 \
leaderboard-rabbitmq
