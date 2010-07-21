import statsmodel
import pbsparse as parser
from glob import glob
from datetime import date
session = statsmodel.Session()
session.autoflush=False
interactive = True
blacklist = ['/var/spool/torque/server_priv/accounting/t.out','/var/spool/torque/server_priv/accounting/20070730.bz2', '/var/spool/torque/server_priv/accounting/20070818.bz2', '/var/spool/torque/server_priv/accounting/20070819.bz2', '/var/spool/torque/server_priv/accounting/20090319', '/var/spool/torque/server_priv/accounting/20090320']
logpath = '/var/spool/torque/server_priv/accounting/'
logprefix = ''
clustername=u'QUARRY'
def get_filelist():
  filelist = glob(logpath + logprefix + '*')
  filelist.sort()
  for item in (logpath+logprefix+str(x[0]) for x in session.execute("select distinct log_filename from job_transaction where cluster_name='%s'" % clustername)):
    if item in filelist:
      filelist.remove(item)
    if item +'.bz2' in filelist:
      filelist.remove(item+'.bz2')
  for item in blacklist:
    if item in filelist: 
      filelist.remove(item)
  filelist.remove(logpath+date.today().isoformat().replace('-',''))
  return filelist
def add_nodes(filelist = ['aviss.var/20090401']):
  for job in parser.jobs(filelist):
    for nodename in job['nodelist']:
      if not session.query(statsmodel.Node).filter_by(name=nodename).one():
        session.add(statsmodel.Node(name=nodename))
        session.commit()

def insert(filelist=['aviss.var/20090401']):
  for job in parser.jobs(filelist):
     if interactive:
       print job
     queue = session.query(statsmodel.Queue).filter_by(name=job['queue']).one()
     if not queue:
       queue = statsmodel.Queue(name=job['queue'])
       session.add(queue)
     job['queue']=queue
     j = statsmodel.Job()
     for item in (x for x in job.keys() if x != 'nodelist'):
       setattr(j,item,job[item])
     j.cluster_name = clustername 
     session.add(j)
     session.commit()
     jquery = session.query(statsmodel.Job).filter_by(jobid=job['jobid'])
     jquery = jquery.filter_by(step=job['step']).filter_by(cluster_name=clustername)
     jquery = jquery.filter_by(submit_time = job['submit_time']).filter_by(completion_time = job['completion_time'])
     j = jquery.one()
     nodelist = []
     for nodename in job['nodelist']:
       try:
         nodelist.append(session.query(statsmodel.Node).filter_by(name=nodename).one())
       except statsmodel.sqlalchemy.orm.exc.NoResultFound: 
         nodelist.append(statsmodel.Node(name=nodename))
         session.add(nodelist[-1])
       if not nodelist[-1]:
         nodelist[-1] = statsmodel.Node(name=nodename)
     for node in nodelist:
       j.nodes.append(node)
     session.commit()

def add_taskcount(filelist):
  for job in parser.jobs(filelist):
    print job
    jquery = session.query(statsmodel.Job).filter_by(jobid=job['jobid'])
    jquery = jquery.filter_by(step=job['step']).filter_by(cluster_name=clustername)
    jquery = jquery.filter_by(submit_time = job['submit_time']).filter_by(completion_time = job['completion_time'])
    j = jquery.one()
    j.tasks = job['tasks']
  session.commit()


if __name__ == '__main__':
  interactive = False
  filelist = get_filelist()
  filelist.sort()
  insert((x for x in filelist if not x in blacklist))
