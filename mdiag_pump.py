#! /bin/env python
import simpledaemon
import logging
import commands
from xml.dom.minidom import parseString
from carrot.connection import AMQPConnection
from carrot.messaging import Publisher
import time

class MdiagDaemon(simpledaemon.Daemon):
  default_conf = '/root/mdiag_pump.conf'
  section = 'mdiag_pump'
  def convert(self, label, item):
    if not item:
      item = None
    elif label in ['LOAD']:
      item = float(item)
    else:
      item = int(item)
    return  (label,item) 
  def run(self):
    periodminutes = float(self.config_parser.get(self.section, 'periodminutes'))
    amqhost = self.config_parser.get(self.section,'amqhost')
    amqport = int(self.config_parser.get(self.section,'amqport'))
    amquser = self.config_parser.get(self.section,'amquser')
    amqpass = self.config_parser.get(self.section,'amqpass')
    amqvhost = self.config_parser.get(self.section,'amqvhost')
    amqexchange = self.config_parser.get(self.section,'amqexchange')
    routing_key = self.config_parser.get(self.section,'routing_key')

    while 1:
      t = commands.getoutput('mdiag --xml -n')

      dom1 = parseString(t)
      nodes = dom1.firstChild.childNodes
      msg = dict(zip((x.getAttribute('NODEID') for x in nodes),
            map(dict,
                 zip((('NODESTATE',x.getAttribute('NODESTATE')) for x in nodes),
                     (self.convert('RAPROC',x.getAttribute('RAPROC')) for x in nodes),
                     (self.convert('RCPROC',x.getAttribute('RCPROC')) for x in nodes),
                     (self.convert('RAMEM',x.getAttribute('RAMEM')) for x in nodes),
                     (self.convert('RCMEM',x.getAttribute('RCMEM')) for x in nodes),
                     (self.convert('LOAD',x.getAttribute('LOAD')) for x in nodes)))))

      msg = {'ts':time.time(),'data':msg}
      logging.info(`msg`)

      try:
        amqpconn = AMQPConnection(hostname=amqhost, port=amqport,
                             userid=amquser, password=amqpass,
                             vhost=amqvhost)
        publisher = Publisher(connection=amqpconn,exchange=amqexchange, 
                            routing_key=routing_key,exchange_type='topic')
        publisher.send(msg)
        publisher.close()
        amqpconn.close()
      
      time.sleep(60*periodminutes)


if __name__ == '__main__':
  mdiagdaemon = MdiagDaemon()
  mdiagdaemon.main()
