#! /bin/env python2.7
import simpledaemon, logging
from datetime import datetime, timedelta
from dateutil import parser as dateparser
import re
import gzip, bz2
import os
from time import sleep,mktime
from kombu.connection import BrokerConnection
from kombu.messaging import Producer,Exchange

entire_history = 'yes'

logpat = re.compile('(.{19});E;(\d+)(?:-(\d+))?\..*;user=(\S+) (?:account=(\S+))?.*group=(\S+).*queue=(\S+) ctime=\d+ qtime=(\d+) etime=(\d+) start=(\d+) .* exec_host=(\S+) (?:Resource_List.mem=(\S+))?.* Resource_List.walltime=(\d+:\d+:\d+) .*Exit_status=(\S+) .*resources_used.mem=(\d+).*\n(\S+)')
jobstartpat = re.compile('(.{19});S;(\d+)(?:-(\d+))?\..*;user=(\S+) (?:account=(\S+))?.*group=(\S+).*queue=(\S+) ctime=\d+ qtime=(\d+) etime=(\d+) start=(\d+) .* exec_host=(\S+) .* Resource_List.walltime=(\d+:\d+:\d+) .*\n(\S+)')

exechostpat = re.compile('/\d+')

colnames = ('type','completion_time','jobid','step','username','project','group','queue','submit_time','eligibletime','start_time','nodelist','requested_mem','walltime','exit_status','mem','filename')

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
def requested_mem_convert(reqested_mem):
  if requested_mem.endswith('gb'):
    returnvalue = int(requested_mem.replace('gb',''))*1024*1024*1024
  elseif requested_mem.endswith('mb'):
    returnvalue = int(requested_mem.replace('mb',''))*1024*1024
  elseif requested_mem.endswith('kb'):
    returnvalue = int(requested_mem.replace('kb',''))*1024
  elseif requested_mem.endswith('b'):
    returnvalue = int(requested_mem.replace('b',''))
  else:
    returnvalue = 0
  return returnvalue
def walltimeconvert(walltime):
  hours,minutes,seconds = walltime.split(':')
  return (int(hours)*60+int(minutes))*60+int(seconds)
#  return timedelta(0,(int(hours)*60+int(minutes))*60+int(seconds),0)

def gen_cat():
  today = '%d%02d%02d' % (datetime.now().year,datetime.now().month,datetime.now().day)
  filename = '/var/spool/torque/server_priv/accounting/' + today
  if entire_history == 'yes':
    filelist = os.listdir('/var/spool/torque/server_priv/accounting')
    filelist.remove(today)
    for f in filelist:
      s = open('/var/spool/torque/server_priv/accounting/'+f)
      logging.info('Now reading from %s' % f)
      for item in s.readlines():
         yield item + s.name.replace('/var/spool/torque/server_priv/accounting/','')
  s = open(filename,'r')
  logging.info('Now reading from %s' % filename)
  while True:
    for item in s.readlines():
      yield item + s.name.replace('/var/spool/torque/server_priv/accounting/','')
    sleep(60)
    if os.stat(filename).st_size == s.tell() and not today ==  '%d%02d%02d' % (datetime.now().year,datetime.now().month,datetime.now().day):
      today =  '%d%02d%02d' % (datetime.now().year,datetime.now().month,datetime.now().day)
      filename = '/var/spool/torque/server_priv/accounting/' + today
      while not os.access(filename,os.R_OK):
         sleep(60)
      s = open(filename,'r')
      logging.info('Now reading from %s' % filename) 

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

def groups_gen(lines):
  for line in lines:
    if logpat.match(line):
       yield (('exit',))+logpat.match(line).groups()
    elif jobstartpat.match(line):
       g = (('start',))+jobstartpat.match(line).groups()
       yield g[:-1]+(0,)+g[-1:]

def job_start_completion_map(dictseq):
  for d in dictseq:
    if d['type']=='start':
      d['completion_time']+=d['walltime']
    yield d

def jobs():
  lines = gen_cat()
#  groups = (logpat.match(line) for line in lines)
#  tuples = (g.groups() for g in groups if g)
  tuples = groups_gen(lines)
  log = (dict(zip(colnames,t)) for t in tuples)
  log = field_map(log,"completion_time",lambda x: int(dateparser.parse(x).strftime('%s')))
#  log = field_map(log,"submit_time",lambda x: datetime.fromtimestamp(int(x)))
#  log = field_map(log,"start_time",lambda x: datetime.fromtimestamp(int(x)))
#  log = field_map(log,"eligibletime", lambda x: datetime.fromtimestamp(int(x)))
  log = field_map(log,"start_time", int)
  log = field_map(log,"eligibletime", int)
  log = field_map(log,"submit_time", int)
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
  log = field_map(log,"exit_status",unicode)
  log = field_map(log,"mem",int)
  log = field_map(log,"filename",unicode)
  log = field_map(log,"requested_mem",requested_mem_convert)
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
     global entire_history
     entire_history = self.config_parser.get(self.section,'entire_history')
     for job in jobs():
        if job['type']=='start':
           job['completion_time']+=job['walltime']
        producer.revive(channel)
        producer.publish(job)
        logging.debug(`job`)

if __name__ == "__main__":
  pbspumpdaemon = PBSPumpDaemon()
  pbspumpdaemon.main()

