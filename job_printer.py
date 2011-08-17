#import simplejson as json
import json
import sys
from datetime import datetime


from amqplib import client_0_8 as amqp
with  amqp.Connection(host="localhost:5672 ", userid="guest",
    password="guest", virtual_host="/", insist=False) as conn:
  with conn.channel() as chan:
    q = chan.queue_declare(durable=False,exclusive=False,auto_delete=True)
    chan.queue_bind(queue=q[0], exchange='amq.topic',routing_key='log.job.#')

    def print_job(msg):
       d = json.loads(msg.body)
       print d
    tag = chan.basic_consume(q[0],no_ack=True, callback=print_job)

    while True:
      chan.wait()
  
    chan.basic_cancel("tag")

