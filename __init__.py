"""

 This module will load a couple of pre-defined
 Celery 'tasks' into the daemon process running
 on the host of invocation.

 The idea is build a web service that allows 
 a user to request certain functions of RabbitMQ
 over the HTTP protocol.

 This module was built using Pika ver 0.5.2

 Note: The result of invocation of a task will send a HTTP 200 
       along with the desired messages in JSON format.

 RabbitMQ: http://www.rabbitmq.com
 Celery  : http://ask.github.com/celery/index.html

 @author Raymond Tay
 @version 1.0
 @company HP Labs Singapore 2011
 @copyright 2011, all rights reserved

"""

from celery.task import task

import pika
import json
import celeryconfig

@task()
def createExchange(name = '', type = '', retrandq = False):
	"""
	name:     name of exchange. if not assigned, 'default' will be returned
	type:     type of the exchange. valid values are 'direct', 'topic', 'headers', 'fanout'
	          default is 'fanout'
	retrandq: if True, a randomq name will be returned else not returned. Expecting
	          users to send mesages to exchanges instead of queues
	"""
	logger = createExchange.get_logger()
	if name == '':
		name = 'default'

	if type not in ['direct', 'topic', 'headers', 'fanout']:
		type = 'fanout'
	logger.info("%s , %s" % (name, type))
	connection = pika.AsyncoreConnection(pika.ConnectionParameters(host=celeryconfig.BROKER_HOST))
	channel = connection.channel()
	channel.exchange_declare(exchange=str(name), type=str(type))

	qname = None
	if retrandq:
		result = channel.queue_declare(exclusive=True)
		qname = result.queue
		logger.info("About to bind %s to %s" % (name, qname))
		channel.queue_bind(exchange=str(name), queue=str(qname))
	
	connection.close()
	
	# Return the data in JSON format
	from StringIO import StringIO
	io = StringIO()
	json.dump({'xchg': name, 'type': type, 'qname':qname},io, sort_keys=True)
	return io.getvalue()

@task()
def listExchanges():
	"""
	Hooks up to RabbitMQ and returns the list 
	of available exchanges in JSON format

	A caveat is that the Unix user that's running Celery
	must be in the /etc/sudoers. e.g. invoke 'visudo'
	if you can to add this user
	"""

	# List the exchanges on the localhost and parse the data
	# and returning only names of exchanges
	import subprocess, re
	output = subprocess.Popen(["rabbitmqctl", "list_exchanges"], stdout=subprocess.PIPE).communicate()[0]
	regex = re.compile("(?:\\n|\\t)([a-zA-Z.]+)")
	xchgnames = [str(s) for s in re.findall(regex,output) if s not in ['direct','topic', 'headers', 'fanout', '...done.']]

	# Return the data in JSON format
	from StringIO import StringIO
	io = StringIO()
	json.dump({'xchg': xchgnames},io, sort_keys=True)
	return io.getvalue()

@task()
def listQueues():
	"""
	Hooks up to RabbitMQ and returns the list of available
	queues in JSON format

	The caveat here is the same as that in 'listExchanges' where
	the unix user needs to be in the sudoers list
	"""
	# List the exchanges on the localhost and parse the data
	# and returning only names of exchanges
	import subprocess, re
	output = subprocess.Popen(["rabbitmqctl", "list_queues"], stdout=subprocess.PIPE).communicate()[0]
	regex = re.compile("(?:\\n|\\t)([a-zA-Z.]+)")
	qnames = [str(s) for s in re.findall(regex,output) if s not in ['...done.']]

	# Return the data in JSON format
	from StringIO import StringIO
	io = StringIO()
	json.dump({'queues': qnames},io, sort_keys=True)
	return io.getvalue()


@task()
def createNameQ(name = ''):
	"""
	Creates a named queue for the default exchange
	name:	if name is empty, then the default queue is created and returned
			'default'.
	"""
	logger = createQ.get_logger()
	if name == '':
		name = 'default'
	connection = pika.AsyncoreConnection(pika.ConnectionParameters(celeryconfig.BROKER_HOST))
	channel = connection.channel()
	# sends to the default exchange
	channel.queue_declare(queue=name)
	logger.info("[CREATE_Q] msg: %s" % name)
	connection.close()
	# Return the data in JSON format
	from StringIO import StringIO
	io = StringIO()
	json.dump({'qname':name},io, sort_keys=True)
	return io.getvalue()

"""
"""
@task()
def sendMsg(msg, xchg ='',  qname = ''):
	"""
	msg:	Message you want to send
	xchg:	Name of the exchange you want to send it to
	qname:	Name of the queue you want to send it to

	Note:	The task does not validate that either 'xchg' or 'qname'
			is valid in the system
	"""
	logger = sendMsg.get_logger()
	if qname == '':
		qname = 'default'
	connection = pika.AsyncoreConnection(pika.ConnectionParameters(celeryconfig.BROKER_HOST))
	channel = connection.channel()
	# sends to the default exchange
	channel.queue_declare(queue=qname)
	channel.basic_publish(exchange=xchg, routing_key=qname, body=msg)
	logger.info("[SENT] msg: %s" % msg)
	connection.close()

	# Return the data in JSON format
	from StringIO import StringIO
	io = StringIO()
	json.dump({'msg': msg}, io, sort_keys=True)
	return io.getvalue()

"""
 Callback function used by the channel to retrieve
 messages. Rationale to do this is to facilitate
 a use case where the user actually wants to use 
 the retrieved message but the callback functionality
 doesn't allow it to happen.

 usage: once the callback returns, simply say 'callback.body'
        to retrieve the message
"""
class MsgCallback(object):
	def __init__(self, func):
		self.func = func
		self.body = ''
	def __call__(self, *args, **kwargs):
		try:
			# debug message 
			# print "args: %s, kargs:%s" % (args,kwargs)
			# if there are no more queues, KeyError will be thrown
			self.body = args[3]
		except Exception:
			pass
		return self.func(*args, **kwargs)

@MsgCallback
def callback(ch, method, properties, body):
    logger = recvMsg.get_logger()
    logger.info("[RECV] %r" % body)
    ch.basic_ack(delivery_tag=method.delivery_tag)

@task()
def recvMsg(msg, qname = ''):
	logger = recvMsg.get_logger()
	if qname == '':
		qname = 'default'
	connection = pika.AsyncoreConnection(pika.ConnectionParameters(celeryconfig.BROKER_HOST))
	channel = connection.channel()
	# sends to the default exchange
	channel.queue_declare(queue=qname)
	channel.basic_consume(callback, queue=qname)
	connection.close()
	logger.info("[x] conn close. %s" % callback.body)

	# Return the data in JSON format
	from StringIO import StringIO
	io = StringIO()
	json.dump({'msg': callback.body}, io, sort_keys=True)
	return io.getvalue()


