#!/usr/bin/env python

import redis
import random

server = redis.Redis(host = '127.0.0.1')
#for userid in range(1, 100000000):
#  score = round(random.random() * 1000000.0, 2)
#  server.zadd("scores", userid, score)

#maxid = 100000000
maxid = 10000000
startid = 1
step = 1000

for i in range(startid, maxid, step):
  pack = ["scores"]
  hpack = {}
  for s in range(0, step):
    score = round(random.random() * 1000000.0, 2)
    userid = i + s
    pack.append(userid)
    pack.append(score)

    hpack[userid] = "user_" + str(userid)
  #print str(pack)
  server.zadd(*pack)
  server.hmset("names", hpack)

print "Done!"
