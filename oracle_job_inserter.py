import cx_Oracle
from kombu.connection import BrokerConnection
from kombu.messaging import Exchange, Queue, Consumer
from datetime import datetime
exchange = Exchange("amq.topic", "topic", durable=True)
q = Queue("oracle_job_inserter", exchange=exchange, key="log.job.#")
q.routing_key="log.job.#"
dsn = cx_Oracle.makedsn('host',1521,'workspace') 

def ts_literal(ts):
  return datetime.fromtimestamp(ts).isoformat().replace('T',' ')

def process_job(msg,body):
  try:
     print msg
#     print body.delivery_info
     cluster = body.delivery_info["routing_key"].replace("log.job.","").upper()
     cursor.execute("""select queue_record_num from job_queue where QUEUE_NAME='%s' and "cluster"='%s'""" % (msg["queue"],cluster))
     queue_id = cursor.fetchall()
     if not queue_id:
       cursor.execute("""insert into job_queue (QUEUE_NAME,"cluster") VALUES ('%s','%s')""" % (msg["queue"],cluster))
       ora_con.commit()
       cursor.execute("""select queue_record_num from job_queue where QUEUE_NAME='%s' and "cluster"='%s'""" % (msg["queue"],cluster))
       queue_id = cursor.fetchall()
     queue_id = queue_id[0][0]
     if msg['type'] == 'exit':
       if not msg['project']: 
         msg['project']='NULL'
       else:
         msg['project'] = "'" + msg['project'] + "'"
       values = (msg["jobid"],msg["step"],msg["group"],msg['project'],
                 ts_literal(msg["submit_time"]),
                 ts_literal(msg["start_time"]),
                 ts_literal(msg["completion_time"]),
                 ts_literal(msg["eligibletime"]),
                 msg["tasks"],msg["walltime"]%86400,msg["walltime"]/86400,
                 cluster,msg["username"],msg["filename"],
                 queue_id,len(msg["nodelist"]),msg["mem"])
#       print """insert into job_transaction (JOB_ID,JOB_STEP_NO,UNIX_GROUP,PROJECT,SUBMIT_TIME,BEGIN_TIME,COMPLETION_TIME,ELIGIBLETIME,TASK_COUNT,REQ_WALLTIME,CLUSTER_NAME,USER_ID,LOG_FILENAME,QUEUE_ID,NODE_COUNT,MEM_USED) values (%d,%d,'%s','%s',timestamp '%s',timestamp '%s', timestamp '%s',timestamp '%s', %d,interval '%d' second(6) + interval '%d' day(3), '%s','%s','%s',%d,%d,%d)""" % values 
       cursor.execute("""insert into job_transaction (JOB_ID,JOB_STEP_NO,UNIX_GROUP,PROJECT,SUBMIT_TIME,BEGIN_TIME,COMPLETION_TIME,ELIGIBLETIME,TASK_COUNT,REQ_WALLTIME,CLUSTER_NAME,USER_ID,LOG_FILENAME,QUEUE_ID,NODE_COUNT,MEM_USED) values (%d,%d,'%s','%s',timestamp '%s',timestamp '%s', timestamp '%s',timestamp '%s', %d,interval '%d' second(6) + interval '%d' day(3), '%s','%s','%s',%d,%d,%d)""" % values)
       ora_con.commit()
       cursor.execute("select job_transactionid from job_transaction where JOB_ID=%d and JOB_STEP_NO=%d and SUBMIT_TIME=timestamp '%s' and COMPLETION_TIME= timestamp '%s' and CLUSTER_NAME='%s'" % 
                    (msg["jobid"],msg["step"],
                     ts_literal(msg["submit_time"]),
                     ts_literal(msg["completion_time"]),
                     cluster) )
       job_transactionid = cursor.fetchone()[0]
       for node in msg["nodelist"]:
          cursor.execute("""select node_record_num from job_node where "NODE"='%s'""" % node)
          nodeid = cursor.fetchall()
          if not nodeid:
             cursor.execute("""insert into job_node ("NODE") values ('%s')""" % node)
             ora_con.commit()
             cursor.execute("""select node_record_num from job_node where "NODE"='%s'""" % node)
             nodeid = cursor.fetchall()
#          print nodeid
          nodeid = nodeid[0][0]
          cursor.execute("insert into job_transaction_job_node (job_transactionid, node_record_num) values ('%d','%d')" % (job_transactionid, nodeid))
          ora_con.commit()
          

  except cx_Oracle.IntegrityError, exc:
    ora_con.rollback() 

with BrokerConnection("localhost", "guest", "guest", "/") as amqp_con:
  with amqp_con.channel() as channel: 
    with cx_Oracle.Connection("user/password@"+dsn) as ora_con:
        cursor =  ora_con.cursor() 
        consumer = Consumer(channel,q)
        consumer.register_callback(process_job)
        consumer.consume(no_ack=True)
        while True:
          amqp_con.drain_events()

