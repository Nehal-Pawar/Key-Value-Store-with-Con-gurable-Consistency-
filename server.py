import glob
import sys
import os.path
import socket

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
		if key>=0 and key <= 63:
			for i in range(0,3):
				if replica_name[i] != sys.argv[1]:
					transport = TSocket.TSocket(replicas[replica_name[i]][0], replicas[replica_name[i]][1])
					transport = TTransport.TBufferedTransport(transport)
					protocol = TBinaryProtocol.TBinaryProtocol(transport)
					client = FileStore.Client(protocol)

					transport.open()
					response[i]= client.getIN(key)
					transport.close()

				else:
					response[i] = store[key]

			print response
			#rpc 0, 1 and 2

		elif key>=64 and key <=127:
			#rpc 1, 2 and 3
		elif key>=128 and key <= 191:
			#rpc 2, 3 and 0
		else:
			#rpc 3, 0 and 1

		# compare TS of replicas 1,2,3 return key of max TS replica
		#print store[key]
		return max(response)

	def put(self, keyvalue):
		walfile = sys.argv[1] + 'wal'
		if keyvalue.key in store.keys():
			list = []
			f = open(walfile, 'w')
			for key in sorted(store):
				if key != keyvalue.key:
					f.write(str(key) + ' ' + store[key] + '\n')
				else:
					f.write(str(keyvalue.key) + ' ' + keyvalue.value + '\n')
				
					
			store[keyvalue.key] = keyvalue.value

		else:
			f = open(walfile, 'a')
			f.write(str(keyvalue.key) + ' ' + keyvalue.value)
			f.close()


			store[keyvalue.key] = keyvalue.value
			print 'put'
			print store


	def getIN(self,key):
		#print store[key]
		return store[key]

if __name__ == '__main__':
	# No command line arguments needed
	if len(sys.argv) != 5:
		print("Usage:", sys.argv[0], "Branch name", "Port number", "WAL file", "nodes.txt")
		sys.exit()

	# IP address of replica
	print socket.gethostbyname(socket.gethostname())

	#Read nodes file
	with open(nodes) as f:
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

				# later lock this store
				store[key] = value
				
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


