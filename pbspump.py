#! /bin/env python2.7
import simpledaemon
from datetime import datetime, timedelta
from dateutil import parser as dateparser
import re
import gzip, bz2
import os
from time import sleep
from kombu.connection import BrokerConnection
from kombu.messaging import Producer,Exchange


logpat = re.compile('(.{19});E;(\d+)(?:-(\d+))?\..*;user=(\S+) (?:account=(\S+))?.*group=(\S+).*queue=(\S+) ctime=\d+ qtime=(\d+) etime=(\d+) start=(\d+) .* exec_host=(\S+) .* Resource_List.walltime=(\d+:\d+:\d+) .*\n(\S+)')

exechostpat = re.compile('/\d+')

colnames = ('completion_time','jobid','step','username','project','group','queue','submit_time','eligibletime','start_time','nodelist','walltime','filename')

def uniquify(seq, idfun=None): 
    # order preserving
    if idfun is None:
        def idfun(x): return x
    seen = {}
    result = []
    for item in seq:
        marker = idfun(item)
#        marker = item
        # in old Python versions:
        # if seen.has_key(marker)
        # but in new ones:
        if marker in seen: continue
        seen[marker] = 1
        result.append(unicode(item))
    return result
def stepconvert(step):
  if not step:
    return 0
  return int(step)
def walltimeconvert(walltime):
  hours,minutes,seconds = walltime.split(':')
  return (int(hours)*60+int(minutes))*60+int(seconds)
#  return timedelta(0,(int(hours)*60+int(minutes))*60+int(seconds),0)

def gen_cat():
  today = '%d%02d%02d' % (datetime.now().year,datetime.now().month,datetime.now().day)
  filename = '/var/spool/torque/server_priv/accounting/' + today
  s = open(filename,'r')
  while True:
    for item in s.readlines():
      yield item + s.name.replace('/var/spool/torque/server_priv/accounting/','')
    sleep(60)
    if os.stat(filename).st_size == s.tell() and not today ==  '%d%02d%02d' % (datetime.now().year,datetime.now().month,datetime.now().day):
      today =  '%d%02d%02d' % (datetime.now().year,datetime.now().month,datetime.now().day)
      filename = '/var/spool/torque/server_priv/accounting/' + today
      s = open(filename,'r') 

def gen_open(filenames):
  for name in filenames:
    if name.endswith(".gz"):
      yield gzip.open(name)
    elif name.endswith(".bz2"):
      yield bz2.BZ2File(name)
    else:
      yield open(name)

def field_map(dictseq,name,func, dep_name= None):
  if dep_name == None:
    dep_name = name
  for d in dictseq:
    d[dep_name] = func(d[name])
    yield d

def jobs():
  lines = gen_cat()
  groups = (logpat.match(line) for line in lines)
  tuples = (g.groups() for g in groups if g)
  log = (dict(zip(colnames,t)) for t in tuples)
#  log = field_map(log,"completion_time",lambda x: dateparser.parse(x))
#  log = field_map(log,"submit_time",lambda x: datetime.fromtimestamp(int(x)))
#  log = field_map(log,"start_time",lambda x: datetime.fromtimestamp(int(x)))
#  log = field_map(log,"eligibletime", lambda x: datetime.fromtimestamp(int(x)))
  log = field_map(log,"walltime", walltimeconvert)
  log = field_map(log,"jobid",int)
  log = field_map(log,"step",stepconvert)
  log = field_map(log,"nodelist", lambda x: exechostpat.sub('',x))
  log = field_map(log,"nodelist",lambda x: x.split('+'))
  log = field_map(log,"nodelist",lambda x: len(x),dep_name = "tasks")
  log = field_map(log,"nodelist",uniquify)
  log = field_map(log,"username", unicode)
  log = field_map(log,"group",unicode)
  log = field_map(log,"queue",unicode)
  log = field_map(log,"filename",unicode)
  return log

class PBSPumpDaemon(simpledaemon.Daemon):
   default_conf = '/etc/pbs_pump.conf'
   section = 'pbs_pump'
   def run(self):
     amqhost = self.config_parser.get(self.section,'amqhost')
     amqport = int(self.config_parser.get(self.section,'amqport'))
     amquser = self.config_parser.get(self.section,'amquser')
     amqpass = self.config_parser.get(self.section,'amqpass')
     amqvhost = self.config_parser.get(self.section,'amqvhost')
     amqexchange = self.config_parser.get(self.section,'amqexchange')
     routing_key = self.config_parser.get(self.section,'routing_key')
     connection = BrokerConnection(hostname=amqhost, port=amqport,
                             userid=amquser, password=amqpass,
                             virtual_host=amqvhost)
     channel = connection.channel()
     exchange = Exchange(amqexchange,'topic',turable=True)
     producer = Producer(channel,exchange=exchange, 
                            routing_key=routing_key)
     for job in jobs():
        producer.revive(channel)
        producer.publish(job)

if __name__ == "__main__":
  pbspumpdaemon = PBSPumpDaemon()
  pbspumpdaemon.main()

