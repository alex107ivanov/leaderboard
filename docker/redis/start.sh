#!/bin/sh

docker pull redis
docker run -d \
	--name redis \
	-v redis-data-new:/data \
	-p 6379:6379 \
redis
