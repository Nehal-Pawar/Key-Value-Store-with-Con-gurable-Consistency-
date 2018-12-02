#!/usr/bin/python

import sys
import glob
import pdb
import hashlib

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from key_value import Store
from key_value.ttypes import SystemException, KeyValue

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol



def main():

	# Make socket
	transport = TSocket.TSocket(sys.argv[1], int(sys.argv[2]))

	# Buffering is critical. Raw sockets are very slow
	transport = TTransport.TBufferedTransport(transport)

	# Wrap in a protocol
	protocol = TBinaryProtocol.TBinaryProtocol(transport)

	# Create a client to use the protocol encoder
	client = Store.Client(protocol)

	# Connect!
	while 1:
		option = input("Enter 1 for get and 2 for put and 3 to exit : ")

		if option == 1 :  
			transport.open()
			testingReadFile(client)
			transport.close()

		elif option ==  2:
			transport.open()
			testingWriteFile(client )    	
			transport.close()

		elif option == 3:
			break;

		else:
			print 'Invalid choice'
    

def testingReadFile(client):
	key = input('Enter key : ')
	if key in range(0,256):
		value = client.get(key)
		

		# if key not present in any of the replicas


		#else
		print 'value for key ' + str(key) + " is " + value

def testingWriteFile(client):
	keyvalue = KeyValue()
	key = input("Enter key : ")
	if key in range(0,256):
		keyvalue.key = key
		keyvalue.value = raw_input("Enter value : ")
		result = client.put(keyvalue)
		if result == True:
			print 'PUT successful!'
		else:
			print 'PUT failed!'

	else:
		print 'Key should be in range of 0 to 255'


if __name__ == '__main__':
    try:
	#pdb.set_trace()
        main()
    except Thrift.TException, tx:
        print '%s' % tx.message
