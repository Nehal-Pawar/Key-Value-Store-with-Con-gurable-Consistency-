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

	def get(self, key,consistency):
		print 'get'
		
		rep = []
		# find all replicas for this key
		primary_replica = (key/64)
		rep.append(primary_replica)
		
		for i in range(1,3):
			sec_replica = (primary_replica + i) % 4
			rep.append(sec_replica)

		EveryResponse=[]
		
		for i in rep:
			
			thread = threading.Thread(target = self.getHandler, args = (i, key,EveryResponse))
			thread.daemon = True
			thread.start()
			thread.join()

		

		
		print EveryResponse
		
		if int(sys.argv[5]) == 1:
			latest=0.0
			for currentresponse in EveryResponse:# for all the response
				if(currentresponse.time>latest):#for time stamp 1 1 2 chnage value of 1 and 1 / if old value received first
					if(latest!=0.0):
						KV=KeyValue()
						KV.key=key
						KV.value=currentresponse.value
						thread = threading.Thread(target = self.putHandlerrepair, args = (oldCorrectResponse.servername, KV, currentresponse.time))
						thread.daemon = True
						thread.start()
					oldCorrectResponse=currentresponse
					latest=currentresponse.time
				if(currentresponse.time<latest):# for timestamp 2 2 1 change value of 1 / if old value recieved later
					KV=KeyValue()
					KV.key=key
					KV.value=oldCorrectResponse.value
					thread = threading.Thread(target = self.putHandlerrepair, args = (currentresponse.servername, KV, currentresponse.time))
					thread.daemon = True
					thread.start()

		if consistency == 1:
			latest=0.0
			bool_res = False
			for currentresponse in EveryResponse:
				if(currentresponse.time>latest):
					bool_res = True
					return currentresponse.value
				
			if bool_res == False:
				exception = SystemException()
               		        exception.message = 'Consistency level does not meet'
				raise exception
		else:
			latest=0.0
			count = 0
			temparr = [] 
			for currentresponse in EveryResponse:
				
				if(currentresponse.time>0):
					count += 1
					temparr.append([currentresponse.time, currentresponse.value, currentresponse.servername])
					if count == 2:
						print temparr
						temparr.sort(key=lambda x: x[0])
						print temparr
						return temparr[0][1]
			
					
			if count < 2:
				exception = SystemException()
                                exception.message = 'Consistency level does not meet'
                                raise exception



	

	def put(self, KV, consistency):
		timestamp = time.time()
		key = KV.key
		response = [-1,-1,-1,-1]
		count = 0

		#stores replica for a particular key
		rep = []

		# find all replicas for this key
		primary_replica = (key/64)
		rep.append(primary_replica)
		
		for i in range(1,3):
			sec_replica = (primary_replica + i) % 4
			rep.append(sec_replica)
		print 'rep is', rep
			

		pool = ThreadPool(processes=1)

		EveryResponse=[]
		for i in rep:
			thread = threading.Thread(target = self.putHandler, args = (i, KV, timestamp, EveryResponse))
			thread.daemon = True
			thread.start()
			thread.join()
		time.sleep(3);
		print 'EveryResponse', EveryResponse
'



		#hinted handoff
		if int(sys.argv[5]) == 2:
			
			for currentresponse in EveryResponse:# for all the response
				if(currentresponse[1]==False):
					hinted_handoff[currentresponse[0]]=[KV.key,KV.value,timestamp]
			print hinted_handoff


		for x in range(len(EveryResponse)):
			tlist = x
			if EveryResponse[tlist][1] == True:
				count += 1

		print 'count', count
		if consistency == 1:
			if count>=1:
				return True
			else:
				exception = SystemException()
               		        exception.message = 'Consistency level does not meet'
				raise exception
		else:
			if count>=2:
				return True
			else:
				exception = SystemException()
                                exception.message = 'Consistency level does not meet'
                                raise exception


		

		
	def putIN(self, keyvalue, timestamp,servername):
		
		if(servername in hinted_handoff.keys()):#have hint stored
			
			#write the hint back to the  recovered server
			keyval=KeyValue()
			keyval.key = int(hinted_handoff[servername][0])
			keyval.value= hinted_handoff[servername][1]
			thread = threading.Thread(target = self.putHandlerrepair, args = (servername,keyval,hinted_handoff[servername][2]))
			thread.daemon = True
			thread.start()
			
		
		response = False
		walfile = sys.argv[1] + 'wal'
		#if key exist update
		if keyvalue.key in store.keys():
			list = []#unused
			#write new file
			f = open(walfile, 'w')
			for key in sorted(store):
				if key != keyvalue.key:#old values
					f.write(str(key) + ' ' + store[key][0] + ' ' + str(store[key][1]) + '\n')
				else:#new values
					f.write(str(keyvalue.key) + ' ' + keyvalue.value + ' ' + str(timestamp) + '\n')
				
					
			store[keyvalue.key] = [keyvalue.value, timestamp]			
			response = True
			f.close()
			#if key does not exist write new file
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
		#print store[key]
		valuetime = ValueTime()
		if key in store.keys():
			#print store[key]
			
			valuetime.value=(store[key][0])
			valuetime.time= float(store[key][1])
			return valuetime
		else:
			valuetime.value="key not found"
			valuetime.time=0.0
			return valuetime

	def getHandler(self, i,key,response):
		tempres = ValueTime()
		
		if replica_name[i] != sys.argv[1]:
          		try:
				transport = TSocket.TSocket(replicas[replica_name[i]][0], replicas[replica_name[i]][1])
				transport = TTransport.TBufferedTransport(transport)
				protocol = TBinaryProtocol.TBinaryProtocol(transport)
				client = Store.Client(protocol)

				transport.open()
			
				tempres = client.getIN(key)
				tempres.servername=replica_name[i]
				#print tempres 
			
				transport.close()
			except:
				print 'server down'
				tempres.value= "server down"
				tempres.time=0.0
				tempres.servername=replica_name[i]
				#print tempres 

		else:
			tempres = self.getIN(key)
			tempres.servername=replica_name[i]


		response.append(tempres)
		#print response
		
		return response

	def putHandler(self, i, KV, timestamp, response):
		#print replica_name[i] + sys.argv[1]
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
				
				print 'response', tempres
				transport.close()
			except:
				print 'server down'
				ServerBool.append(False)		

		else:
			#pdb.set_trace()
			tempres=self.putIN(KV, timestamp,replica_name[i])#added replica name for other server to search for hinted hand off in his dict 
			ServerBool.append(tempres)

		response.append(ServerBool)#store values becaue cant return and wait
		return response



	def putHandlerrepair(self,servername,KV,timestamp):
		#print replica_name[i] + sys.argv[1]
		print servername
		if servername != sys.argv[1]:
          		try:
				print replicas[servername][0]
				print replicas[servername][1]
				transport = TSocket.TSocket(replicas[servername][0], replicas[servername][1])
				transport = TTransport.TBufferedTransport(transport)
				protocol = TBinaryProtocol.TBinaryProtocol(transport)
				client = Store.Client(protocol)
				
				transport.open()
				
				tempres=client.putIN(KV, timestamp,servername)
				
				print 'response', tempres
				transport.close()
			except:
				print 'server down'
				return False

		else:			
			tempres=self.putIN(KV, timestamp,servername)
		

if __name__ == '__main__':
	# Command line arguments needed
	if len(sys.argv) != 6:
		print("Usage:", sys.argv[0], "Branch name", "Port number", "WAL file", "nodes.txt","1 = read repair:2 = hinted handoff ")
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
