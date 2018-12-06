import glob
import sys
import os.path
import socket
import time
#import thread
import logging
from threading import Thread
import threading

logging.basicConfig()

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from key_value import Store
from key_value.ttypes import SystemException, KeyValue, ValueTime

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

store = {}
hinted_handoff={}
replica_name = []
replicas = {}

class StoreHandler():

	def get(self, key, consistency):
		rep = []
		# find all replicas for this key
		primary_replica = (key/64)
		rep.append(primary_replica)
		
		for i in range(1,3):
			sec_replica = (primary_replica + i) % 4
			rep.append(sec_replica)

		EveryResponse=[]
		
		# Contact all replicas of the key
		for i in rep:
			thread = threading.Thread(target = self.getHandler, args = (i, key, EveryResponse))
			thread.daemon = True
			thread.start()
			thread.join()

		# If Read Repair is enabled
		if int(sys.argv[4]) == 1:
			latest=0.0
			for currentresponse in EveryResponse:# for all the response

				# if old value received first
				if(currentresponse.time > latest):
					if(latest != 0.0):
						KV = KeyValue()
						KV.key = key
						KV.value = currentresponse.value
						thread = threading.Thread(target = self.putHandlerrepair, args = (oldCorrectResponse.servername, KV, currentresponse.time))
						thread.daemon = True
						thread.start()
					oldCorrectResponse=currentresponse
					latest=currentresponse.time

				# if old value recieved later
				if(currentresponse.time < latest):
					KV = KeyValue()
					KV.key = key
					KV.value = oldCorrectResponse.value
					thread = threading.Thread(target = self.putHandlerrepair, args = (currentresponse.servername, KV, currentresponse.time))
					thread.daemon = True
					thread.start()

		# If Consistency level is ONE
		if consistency == 1:
			latest = 0.0
			bool_res = False
			for currentresponse in EveryResponse:

				# Return the first successful response
				if currentresponse.time > latest:
					bool_res = True
					return currentresponse.value

			# If all replicas are not up	
			if bool_res == False:
				exception = SystemException()
               		        exception.message = 'EXCEPTION: Consistency level does not meet'
				raise exception

		# If Consistency level is QUORUM
		else:
			latest=0.0
			count = 0
			temparr = [] 
			for currentresponse in EveryResponse:
				
				if(currentresponse.time>0):
					count += 1
					temparr.append([currentresponse.time, currentresponse.value, currentresponse.servername])
					if count == 2:
						temparr.sort(key=lambda x: x[0])
						return temparr[0][1]
			
			# If 2 replicas are not up	
			if count < 2:
				exception = SystemException()
                                exception.message = 'EXCEPTION: Consistency level does not meet'
                                raise exception



	def put(self, KV, consistency):
		timestamp = time.time()
		key = KV.key
		count = 0

		#stores replica for a particular key
		rep = []
		# find all replicas for this key
		primary_replica = (key/64)
		rep.append(primary_replica)
		
		for i in range(1,3):
			sec_replica = (primary_replica + i) % 4
			rep.append(sec_replica)
		
		# Contact all replicas
		EveryResponse=[]
		for i in rep:
			thread = threading.Thread(target = self.putHandler, args = (i, KV, timestamp, EveryResponse))
			thread.daemon = True
			thread.start()
			thread.join()
		time.sleep(1)
		
		#Hinted Handoff mechanism
		if int(sys.argv[4]) == 2:
			
			for currentresponse in EveryResponse:# for all the response
				# If any replica is failed store its data locally
				if(currentresponse[1]==False):
					hinted_handoff[currentresponse[0]]=[KV.key,KV.value,timestamp]

		# Count successful writes
		for x in range(len(EveryResponse)):
			if EveryResponse[x][1] == True:
				count += 1

		# If Consistency level is ONE
		if consistency == 1:
			if count>=1:
				return True
			else:
				exception = SystemException()
               		        exception.message = 'Consistency level does not meet'
				raise exception

		# If Consistency level is QUORUM
		else:
			if count>=2:
				return True
			else:
				exception = SystemException()
                                exception.message = 'Consistency level does not meet'
                                raise exception

		
	def putIN(self, keyvalue, timestamp, servername):
		# if server has hint stored
		if(servername in hinted_handoff.keys()):
			
			#write the hint back to the recovered server
			keyval = KeyValue()
			keyval.key = int(hinted_handoff[servername][0])
			keyval.value = hinted_handoff[servername][1]
			thread = threading.Thread(target = self.putHandlerrepair, args = (servername, keyval, hinted_handoff[servername][2]))
			thread.daemon = True
			thread.start()
			
		response = False
		walfile = sys.argv[1] + 'wal'
		# if key exist then update its value
		if keyvalue.key in store.keys():
			# Write updated value in log file
			f = open(walfile, 'w')
			for key in sorted(store):
				if key != keyvalue.key: # old values
					f.write(str(key) + ' ' + store[key][0] + ' ' + str(store[key][1]) + '\n')
				else: # new values
					f.write(str(keyvalue.key) + ' ' + keyvalue.value + ' ' + str(timestamp) + '\n')
				
					
			store[keyvalue.key] = [keyvalue.value, timestamp]			
			response = True
			f.close()

		# if key does not exist append to log file
		else:
			f = open(walfile, 'a')
			f.write(str(keyvalue.key) + ' ' + keyvalue.value + ' ' + str(timestamp) + '\n')
			f.close()
			store[keyvalue.key] = [keyvalue.value, timestamp]			
			response = True

		print 'put successful'
		print store
		
		return response
		


	def getIN(self,key):

		valuetime = ValueTime()
		if key in store.keys():
			valuetime.value = (store[key][0])
			valuetime.time = float(store[key][1])
			return valuetime
		else:
			valuetime.value = "key not found"
			valuetime.time = 0.0
			return valuetime


	def getHandler(self, i, key, response):
		tempres = ValueTime()
		
		# RPC to replicas
		if replica_name[i] != sys.argv[1]:
          		try:
				transport = TSocket.TSocket(replicas[replica_name[i]][0], replicas[replica_name[i]][1])
				transport = TTransport.TBufferedTransport(transport)
				protocol = TBinaryProtocol.TBinaryProtocol(transport)
				client = Store.Client(protocol)

				transport.open()
			
				tempres = client.getIN(key)
				tempres.servername=replica_name[i]
				transport.close()
			except:
				print 'server down'
				tempres.value= "server down"
				tempres.time=0.0
				tempres.servername=replica_name[i]

		# Replica is co-ordinator
		else:
			tempres = self.getIN(key)
			tempres.servername=replica_name[i]


		response.append(tempres)
		
		return response

	def putHandler(self, i, KV, timestamp, response):
		
		ServerBool=[]
		ServerBool.append(replica_name[i])
		if replica_name[i] != sys.argv[1]:
          		try:
				transport = TSocket.TSocket(replicas[replica_name[i]][0], replicas[replica_name[i]][1])
				transport = TTransport.TBufferedTransport(transport)
				protocol = TBinaryProtocol.TBinaryProtocol(transport)
				client = Store.Client(protocol)

				transport.open()
				
				tempres=client.putIN(KV, timestamp,sys.argv[1])
				ServerBool.append(tempres)
				
				transport.close()
			except:
				print 'server down'
				ServerBool.append(False)		

		else:
			tempres=self.putIN(KV, timestamp,replica_name[i])#added replica name for other server to search for hinted hand off in his dict 
			ServerBool.append(tempres)

		response.append(ServerBool)
		return response



	def putHandlerrepair(self,servername,KV,timestamp):
		if servername != sys.argv[1]:
          		try:
				transport = TSocket.TSocket(replicas[servername][0], replicas[servername][1])
				transport = TTransport.TBufferedTransport(transport)
				protocol = TBinaryProtocol.TBinaryProtocol(transport)
				client = Store.Client(protocol)
				
				transport.open()
				
				tempres=client.putIN(KV, timestamp,servername)
				
				transport.close()
			except:
				print 'server down'
				return False

		else:			
			tempres=self.putIN(KV, timestamp,servername)
		

if __name__ == '__main__':
	# Command line arguments needed
	if len(sys.argv) != 5:
		print("Usage:", sys.argv[0], "Branch name", "Port number", "Nodes File", "1 : Read-Repair; 2 : Hinted-Handoff ")
		sys.exit()

	# IP address and port number of replica
	print sys.argv[1] + ' running on IP : ' + socket.gethostbyname(socket.gethostname()) + ' and PORT : ' + sys.argv[2]

	#Read nodes file
	with open(sys.argv[3]) as f:
		for line in f:
			name = line.split()[0]
			replica_name.append(name)
			temp_list = []
			temp_list.append(line.split()[1])
			temp_list.append(line.split()[2])
			replicas[name] = temp_list


	# Reading write-ahead log file
	WAL = sys.argv[1] + 'wal'
	# if write-ahead log file present
	if os.path.isfile(WAL):

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
		print 'Write-ahead log file is not present'


	handler = StoreHandler()
	processor = Store.Processor(handler)
	transport = TSocket.TServerSocket(port=int(sys.argv[2]))
	tfactory = TTransport.TBufferedTransportFactory()
	pfactory = TBinaryProtocol.TBinaryProtocolFactory()

	server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

	print('Starting the server...')
	server.serve()
	print('done.')
