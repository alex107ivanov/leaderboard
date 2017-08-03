#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pika
import time
import random

import protocol_pb2

credentials = pika.PlainCredentials("admin", "password")

connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1', 5672, "/", credentials))
channel = connection.channel()

while True:
  message = protocol_pb2.UserRegistered()
  message.userid = int(random.random()*1000000)
  message.name = "user_"+str(message.userid)
  channel.basic_publish(exchange = 'user_registered', routing_key = '', body = message.SerializeToString())
  #
  message = protocol_pb2.UserRenamed()
  message.userid = int(random.random()*1000000)
  message.name = "user_0"+str(message.userid)
  channel.basic_publish(exchange = 'user_renamed', routing_key = '', body = message.SerializeToString())
  #
  message = protocol_pb2.UserDeal()
  message.userid = int(random.random()*1000000)
  message.time = int(time.time())
  message.amount = round(random.random()*100, 2)
  channel.basic_publish(exchange = 'user_deal', routing_key = '', body = message.SerializeToString())
  #
  message = protocol_pb2.UserDealWon()
  message.userid = int(random.random()*1000000)
  message.time = int(time.time())
  message.amount = round(random.random()*100, 2)
  channel.basic_publish(exchange = 'user_deal_won', routing_key = '', body = message.SerializeToString())
  #
  message = protocol_pb2.UserConnected()
  message.userid = int(random.random()*1000000)
  channel.basic_publish(exchange = 'user_connected', routing_key = '', body = message.SerializeToString())
  #
  message = protocol_pb2.UserDisconnected()
  message.userid = int(random.random()*1000000)
  channel.basic_publish(exchange = 'user_disconnected', routing_key = '', body = message.SerializeToString())
  #
