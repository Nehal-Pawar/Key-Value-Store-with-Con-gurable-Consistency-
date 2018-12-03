import glob
import sys
import os.path
import socket
import time
import thread
import logging
import pdb;
import Queue
from threading import Thread
import threading
from multiprocessing.pool import ThreadPool
logging.basicConfig()

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from key_value import Store
from key_value.ttypes import SystemException, KeyValue

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer


store = {}
replica_name = []
replicas = {}

class StoreHandler():

	def get(self, key,consistency):
		print 'get'
		response = [-1,-1,-1,-1]
		rep = []
		
		# find all replicas for this key
		primary_replica = (key/64)
		rep.append(primary_replica)
		#print 'primary replica is ' , str(primary_replica)

		for i in range(1,3):
			sec_replica = (primary_replica + i) % 4
			rep.append(sec_replica)
			#print 'sec replica is ' , str(sec_replica)

		print 'rep is', rep
		q=Queue.Queue()
		#thread=[-1,-1,-1,-1]
		for i in rep:
			#thread = pool.apply_async(self.putHandler, (i, KV, timestamp))
			thread = threading.Thread(target = self.getHandler, args = (i, key,q))
			thread.daemon = True
			thread.start()
			
		if consistency == 1:
			while(q.empty()):
				print''	
			response=q.get()
			return response
		else:
			while(q.qsize()!=2):
				print ''
			response=q.get()
			return response
			
		print response
		#if(response=-1)
		#	return "key not found"

		# compare TS of replicas 1,2,3 return key of max TS replica
		#print store[key]
		return max(response)

	

	def put(self, KV, consistency):
		timestamp = time.time()
		key = KV.key
		response = [-1,-1,-1,-1]
		count = 0
		rep = []
		
		# find all replicas for this key
		primary_replica = (key/64)
		rep.append(primary_replica)
		#print 'primary replica is ' , str(primary_replica)

		for i in range(1,3):
			sec_replica = (primary_replica + i) % 4
			rep.append(sec_replica)
			#print 'sec replica is ' , str(sec_replica)
		pool = ThreadPool(processes=1)
		print 'rep is', rep
		q=Queue.Queue()
		#thread=[-1,-1,-1,-1]
		for i in rep:
			#thread = pool.apply_async(self.putHandler, (i, KV, timestamp))
			thread = threading.Thread(target = self.putHandler, args = (i, KV, timestamp,q))
			thread.daemon = True
			thread.start()
			
		#if consistency == 1:
		#	while(1)				
		#		for i in rep:
		#			response[i] = thread[i].get
		#			if response[i] == True:
		#				break
		if consistency == 1:
			while(q.empty()):
				print''	
			response=q.get()
			return response
		else:
			while(q.qsize()!=2):
				print ''
			response=q.get()
			return response
		#print response
		#print count

		#if consistency == 1:
		#	if count >= 1:
		#		del rep[:]
		#		return True
		#	else:
		#		return False
		
		#else:
		#	if count >= 2:
		#		del rep[:]
		#		return True
		#	else:
		#		return False
			
		



	def putIN(self, keyvalue, timestamp):
		response = False
		walfile = sys.argv[1] + 'wal'
		#if key exist update
		if keyvalue.key in store.keys():
			list = []#unused
			#write new file
			f = open(walfile, 'w')
			for key in sorted(store):
				if key != keyvalue.key:#old values
					f.write(str(key) + ' ' + store[key][0] + ' ' + store[key][1] + '\n')
				else:#new values
					f.write(str(keyvalue.key) + ' ' + keyvalue.value + ' ' + str(timestamp) + '\n')
				
					
			store[keyvalue.key] = [keyvalue.value, timestamp]
			#store[keyvalue.key][1] = timestamp
			response = True
			f.close()
			#if key does not exist write new file
		else:
			f = open(walfile, 'a')
			f.write(str(keyvalue.key) + ' ' + keyvalue.value + ' ' + str(timestamp) + '\n')
			f.close()
			store[keyvalue.key] = [keyvalue.value, timestamp]
			#store[keyvalue.key][1] = timestamp
			response = True

		print 'put sucessful'
		print store
		
		return response
		


	def getIN(self,key):
		#print store[key]
		if key in store.keys():
			return store[key][0]
		else:
			return "key not found"

	def getHandler(self, i,key):
		if replica_name[i] != sys.argv[1]:
          
			transport = TSocket.TSocket(replicas[replica_name[i]][0], replicas[replica_name[i]][1])
			transport = TTransport.TBufferedTransport(transport)
			protocol = TBinaryProtocol.TBinaryProtocol(transport)
			client = Store.Client(protocol)

			transport.open()
			response = client.getIN(key)
			transport.close()

		else:
			response = self.getIN(key)

		return response

	def putHandler(self, i, KV, timestamp,response):
		#print replica_name[i] + sys.argv[1]
		if replica_name[i] != sys.argv[1]:
          		try:
				transport = TSocket.TSocket(replicas[replica_name[i]][0], replicas[replica_name[i]][1])
				transport = TTransport.TBufferedTransport(transport)
				protocol = TBinaryProtocol.TBinaryProtocol(transport)
				client = Store.Client(protocol)

				transport.open()
				tempres=client.putIN(KV, timestamp)
				response.put(tempres)
				print 'response', tempres
				transport.close()
			except:
				print 'server down'
				return False

		else:
			#pdb.set_trace()
			tempres=self.putIN(KV, timestamp)
			response.put(tempres)

		#return response

	

if __name__ == '__main__':
	# Command line arguments needed
	if len(sys.argv) != 5:
		print("Usage:", sys.argv[0], "Branch name", "Port number", "WAL file", "nodes.txt")
		sys.exit()

	# IP address of replica
	print socket.gethostbyname(socket.gethostname())

	#Read nodes file
	with open("nodes") as f:
		for line in f:
			name = line.split()[0]
			replica_name.append(name)
			temp_list = []
			temp_list.append(line.split()[1])
			temp_list.append(line.split()[2])
			replicas[name] = temp_list


	
	WAL = sys.argv[3]

	# if write-ahead log file present
	if os.path.isfile(sys.argv[3]):

		# store contents of write-ahead log file in memory
		with open(WAL) as f:
			for line in f:  
				key = int(line.split()[0])   
				value = line.split()[1]  
				timestamp = line.split()[2]
				# later lock this store
				store[key] = [value, timestamp]
				
				
		f.close()
		print store

	else:
		print 'no wal file'


	handler = StoreHandler()
	processor = Store.Processor(handler)
	transport = TSocket.TServerSocket(port=int(sys.argv[2]))
	tfactory = TTransport.TBufferedTransportFactory()
	pfactory = TBinaryProtocol.TBinaryProtocolFactory()

	server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

	print('Starting the server...')
	server.serve()
	print('done.')
