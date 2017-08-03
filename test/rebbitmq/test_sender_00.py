#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pika
import time
import random

import protocol_pb2

credentials = pika.PlainCredentials("admin", "password")

connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1', 5672, "/", credentials))
channel = connection.channel()

#
#message = protocol_pb2.UserConnected()
#message.userid = 500
#channel.basic_publish(exchange = 'user_connected', routing_key = '', body = message.SerializeToString())
#
message = protocol_pb2.UserDisconnected()
message.userid = 500
channel.basic_publish(exchange = 'user_disconnected', routing_key = '', body = message.SerializeToString())
#
