import cherrypy
from celery.task import task
from celery.execute import send_task


"""

 This class is named "World" as its meant
 to be facing the public. For the lack of a
 better name, really.

 This object, World, is a CherryPy server 
 running at the port as defined in the 'cherrypy.conf'

 To get to know CherryPy better, please checkout:
 http://www.cherrypy.org/
 
 or you can skip to CherryPy's wiki page:
 http://www.cherrypy.org/wiki/

 @author Raymond Tay
 @version 1.0
 @company HP Labs Singapore
 @copyright 2011, all rights reserved

"""
class World(object):

 def index(self):
  return ""

 """

 It understands the following urls but the keyword after
 'action' should be in ['recvMsg', 'sendMsg', 'createXchg', 'listXchgs']

 http://<url>:<port>/action?action=recvMsg...&var1=val1&var2=val2
 http://<url>:<port>/action?action=recvMsg...&var1=val1&var2=val2
 http://<url>:<port>/action?action=createXchg...&var1=val1&var2=val2
 http://<url>:<port>/action?action=getXchgs...&var1=val1&var2=val2
  
 """
 def action(self,action,*args, **kwargs):
   msg = None
   q = None
   xchgname = None
   xchgtype = None
   retrandq = None

   try:
    xchgname = kwargs['name']
    xchgtype = kwargs['type']
    retrandq = kwargs['retrandq']
   except Exception, e:
    pass

   try:
   	msg = kwargs['msg']
   	q   = kwargs['q']
   except Exception,e:
    pass
   if q == '\'\'' or q == '' or q == None:
        q = 'default'  

   if action == "recvMsg":
    from celery.execute import send_task
    print("About to send task...msg:%s,q:%s" % (msg,q))
    result = send_task("WebBroker.recvMsg", [msg, q])
    print("Got result..")
    return str(result.get())
   if action == "sendMsg":
    from celery.execute import send_task
    print("About to send task...msg:%s,q:%s" % (msg,q))
    result = send_task("WebBroker.sendMsg", [msg, q])
    print("Got result..")
    return str(result.get())
   elif action == "createXchg":
    from celery.execute import send_task
    result = send_task("WebBroker.createExchange", [xchgname, xchgtype, retrandq])
    return str(result.get())
   elif action == "getXchgs":
    from celery.execute import send_task
    result = send_task("WebBroker.listExchanges", [])
    return str(result.get())

 # Exposing the methods for invocations
 index.exposed = True
 action.exposed = True

#
# By default, when this process is being killed or shutdown
# the tasks will be destroyed and no graceful degradation 
# will be implemented.
if __name__ == "__main__":
 import os.path
 conf = os.path.join(os.path.dirname(__file__), 'cherrypy.conf')
 cherrypy.config.update(conf)
 cherrypy.quickstart(World())
 

