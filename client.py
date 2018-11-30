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

    transport.open()
    testingWriteFile(client )    	
    transport.close()

    transport.open()
    testingReadFile(client)
    transport.close()

def testingReadFile(client ):
	key = 1
	value = client.get(key)
	print (value)


def testingWriteFile(client ):
    keyvalue = KeyValue()
    keyvalue.key=1
    keyvalue.value="hello"
    client.put(keyvalue)
	
       # Close!




if __name__ == '__main__':
    try:
	#pdb.set_trace()
        main()
    except Thrift.TException, tx:
        print '%s' % tx.message
