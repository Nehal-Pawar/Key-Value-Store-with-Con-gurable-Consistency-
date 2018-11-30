import sys
import os.path
import socket

store = {}


def get(self, key):
		print 'get'
		print store[key]
		return store[key]

def put(self, keyvalue):
		store[keyvalue.key] = keyvalue.value
		print 'put'
		print store


if __name__ == '__main__':
	# No command line arguments needed
	if len(sys.argv) != 5:
		print("Usage:", sys.argv[0], "Branch name", "Port number", "WAL file", "nodes.txt")
		sys.exit()


	print socket.gethostbyname(socket.gethostname())
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
				

		print store

	else:
		print 'no wal file'
