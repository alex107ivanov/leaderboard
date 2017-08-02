#!/bin/bash

docker run -it --rm --link redis:redis redis redis-cli -h redis -p 6379