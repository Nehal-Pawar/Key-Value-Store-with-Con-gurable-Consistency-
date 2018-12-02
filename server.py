import glob
import sys
import os.path
import socket
import time
import thread
import logging
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

	def get(self, key):
		print 'get'
		response = [-1,-1,-1]

		for i in range(0,4):
			print i
			response[i] = getHandler(i)
			
		print response
		


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

		print 'rep is', rep
		
		for i in rep:
			response[i] = self.putHandler(i, KV, timestamp)
			if response[i] == True:
				count += 1
			
		print response
		print count

		if consistency == 1:
			if count >= 1:
				del rep[:]
				return True
			else:
				return False
		else:
			if count >= 2:
				del rep[:]
				return True
			else:
				return False
			
		



	def putIN(self, keyvalue, timestamp):
		response = False
		walfile = sys.argv[1] + 'wal'
		if keyvalue.key in store.keys():
			list = []
			f = open(walfile, 'w')
			for key in sorted(store):
				if key != keyvalue.key:
					f.write(str(key) + ' ' + store[key][0] + ' ' + store[key][1] + '\n')
				else:
					f.write(str(keyvalue.key) + ' ' + keyvalue.value + ' ' + str(timestamp) + '\n')
				
					
			store[keyvalue.key] = [keyvalue.value, timestamp]
			#store[keyvalue.key][1] = timestamp
			response = True
			f.close()

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


	def getHandler(self, i):
		if replica_name[i] != sys.argv[1]:
          
			transport = TSocket.TSocket(replicas[replica_name[i]][0], replicas[replica_name[i]][1])
			transport = TTransport.TBufferedTransport(transport)
			protocol = TBinaryProtocol.TBinaryProtocol(transport)
			client = Store.Client(protocol)

			transport.open()
			response = client.getIN(key)
			transport.close()

		else:
			if key in store.keys():
				response = store[key][0]

		return response

	def putHandler(self, i, KV, timestamp):
		if replica_name[i] != sys.argv[1]:
          		try:
				transport = TSocket.TSocket(replicas[replica_name[i]][0], replicas[replica_name[i]][1])
				transport = TTransport.TBufferedTransport(transport)
				protocol = TBinaryProtocol.TBinaryProtocol(transport)
				client = Store.Client(protocol)

				transport.open()
				response = client.putIN(KV, timestamp)
				print 'response', response
				transport.close()
			except:
				print 'server down'
				return False

		else:
			response = self.putIN(KV, timestamp)

		return response

	

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
