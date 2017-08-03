#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pika
import datetime
import time

import protocol_pb2

credentials = pika.PlainCredentials("admin", "password")

connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1', 5672, "/", credentials))
channel = connection.channel()

#channel.exchange_declare(exchange='gate.GprsMessage', type='fanout')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='user_info', queue=queue_name)

print ' [*] Waiting for messages. To exit press CTRL+C'

def callback(ch, method, properties, body):
    global time
    ti = time.time()
    st = datetime.datetime.fromtimestamp(ti).strftime('%Y-%m-%d %H:%M:%S')
    print st, 'We got new message'
    #print " [x] %r" % (body,)
    message = protocol_pb2.UserInfo()
    message.ParseFromString(body)

    print '  userid: ', message.userid

channel.basic_consume(callback, queue=queue_name, no_ack=True)

channel.start_consuming()
