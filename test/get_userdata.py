#!/usr/bin/env python

import redis
import random
import timeit

server = redis.Redis(host = '127.0.0.1')

def get(userid):

  score = server.zscore("scores", userid)

  if score == None:
    print "Userid ", userid, " not found."
    return

  place = server.zrevrank("scores", userid)
  start = 0 if place <= 10 else place - 10
  stop = place + 10

  result = server.zrevrange("scores", start, stop, withscores=True)

  top = server.zrevrange("scores", 0, 10, withscores=True)

  name = server.hget("names", userid)

  print "Done!", "\n", "Userid: ", userid, "\n", "Name: ", name, "\n", "Score:", "\n", score, "\n", "Place:", "\n", place, "\n", "Closest:", "\n", result, "\n", "Top 10:", "\n", top



#userid = int(random.random() * 100000000)
def doit():
  #userid = int(random.random() * 60000000)
  userid = int(random.random() * 10000000)

  get(userid)

cnt = 10000

print "%.4f msec" %(timeit.timeit(doit, number=cnt) / cnt * 1000)

