#!/usr/bin/env python
"""
Description:
The XmlRpc API for this library is:
  get(base64 key)
    Returns the value and ttl associated with the given key using a dictionary
      or an empty dictionary if there is no matching key
    Example usage:
      rv = rpc.get(Binary("key"))
      print rv => {"value": Binary, "ttl": 1000}
      print rv["value"].data => "value"
  put(base64 key, base64 value, int ttl)
    Inserts the key / value pair into the hashtable, using the same key will
      over-write existing values
    Example usage:  rpc.put(Binary("key"), Binary("value"), 1000)
  print_content()
    Print the contents of the HT
  read_file(string filename)
    Store the contents of the Hahelperable into a file
  write_file(string filename)
    Load the contents of the file into the Hahelperable
"""

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
from socket import error as socket_error
import random

# Presents a HT interface
class SimpleHT(SimpleXMLRPCServer.SimpleXMLRPCServer):
  def __init__(self, port):
    self.kill = False
    SimpleXMLRPCServer.SimpleXMLRPCServer.__init__(self, ('',port))
    self.data = {}
    self.next_check = datetime.now() + timedelta(minutes = 5)
    #call the polling function that returns correct url to take backup from

    backup_correct_url = self.polling()
    if backup_correct_url:
    	self.backup_from_peer(backup_correct_url)

  def polling(self):
   try:
    if (len(sys.argv) > 2):
          serverurls = []				#list of all other urls/ peer servers 
	  for i in xrange(2,len(sys.argv)):
    		url_string = "http://localhost:"+sys.argv[i]
   		serverurls.append(url_string)
          count = 0
	  servers = []					#a list to contain data from all servers to compare later
          server_set_list = []				#to keep track of server flags, 1:correct, 0:crashed/corrupted
          quorum_server_list = []
	  one = 1
	  zero = 0	
          length = len(serverurls)
    	
	  for url in serverurls:
		servers.append(xmlrpclib.ServerProxy(url))		#make Server proxy for all urls in list
	  
	  for server_s in servers:
		server_values = []					#list to contain keys and values from all urls
                backup_data = pickle.loads(server_s.print_content())	#call print_content to get self.data for all urls
		for key in backup_data:
			val = backup_data[key]				#separate key from self.data
			print val, val[0]				#separate value from the tuple so that there is no ttl
			server_values.append(val[0])			#append the list with keys and values from all servers
	
                quorum_server_list.append(server_values)		#append the quorum
		
	  for i in range(len(quorum_server_list)):
                   server_set_list.append(0)
		   for j in range(len(quorum_server_list)):
                  		if ((quorum_server_list[i] == quorum_server_list[j]) and (i != j)):
                      			server_set_list[i] = server_set_list[i] | one
                  		else:
					server_set_list[i] = server_set_list[i] | zero  
                                        
          count = server_set_list.count(1)            
          print server_set_list, "set"				        #print the list containing server flags,1:correct,0:crash
        
	  print count, "....count...."
          for item in server_set_list:
			if item == 1:
				index = server_set_list.index(item)	#if flag = 1, backup from that url
				backup_url = serverurls[index]
			break
          	
          if count >= len(quorum_server_list):			        #if majority of the urls paas the same data then take backup
		return backup_url				        #return correct url to polling
	  else:
		return 0
   except socket_error as serr:
    	print "connectionerror"

  def count(self):
    # Remove expired entries
    self.next_check = datetime.now() - timedelta(minutes = 5)
    self.check()
    return len(self.data)

  "added for testing"
  def list_contents(self):
        print self.data.keys()
        return self.data.keys()
        

  "added for testing"
  def corrupt(self, key):
        print key
  	self.check()
	self.put(Binary(key),Binary(pickle.dumps('datacorrupted')),6000)
        return 1

  def terminate(self):
	self.kill = True
	return 1

  def serve_forever(self):
	while not self.kill:
		self.handle_request()

  # Retrieve something from the HT
  def get(self, key):
    # Remove expired entries
    self.check()
    # Default return value
    rv = {}
    # If the key is in the data structure, return properly formated results
    key = key.data
    if key in self.data:
      ent = self.data[key]
      now = datetime.now()
      if ent[1] > now:
        ttl = (ent[1] - now).seconds
        rv = {"value": Binary(ent[0]), "ttl": ttl}
      else:
        del self.data[key]
    return rv

  # Insert something into the HT
  def put(self, key, value, ttl):
    # Remove expired entries
    self.check()
    end = datetime.now() + timedelta(seconds = ttl)
    self.data[key.data] = (value.data, end)
    return True
	

  # Load contents from a file
  def read_file(self, filename):
    f = open(filename.data, "rb")
    self.data = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename.data, "wb")
    pickle.dump(self.data, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.data
    return pickle.dumps(self.data)
 
  # Remove expired entries
  def check(self):
    now = datetime.now()
    if self.next_check > now:
      return
    self.next_check = datetime.now() + timedelta(minutes = 5)
    to_remove = []
    for key, value in self.data.items():
      if value[1] < now:
        to_remove.append(key)
    for key in to_remove:
      del self.data[key]
   
  def backup_from_peer(self, url):
      print url						#correct url achieved from polling
      try:
      	rpc = xmlrpclib.ServerProxy(url)
	dat = rpc.print_content()
      	self.data = pickle.loads(dat)
      except socket_error as serr:
		print "conncectionerror"

  def repair_corrupt(self, url):
    try:
    	rpc = xmlrpclib.ServerProxy(url)		#this rpc instance refers to the corrupted server
        correct_data = rpc.print_content()
    	self.data = pickle.loads(correct_data)
        self.check()
    	print "took the content from correct server.... " 

    except socket_error as serr:
	print "connectionerror"

      
       
def main():
  
  if (len(sys.argv) < 2):
     print "Input Argument Error:port local port, data servers"
  port = int(sys.argv[1])
  
  serve(port)

# Start the xmlrpc server
def serve(port):
  #file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  
  print "I am connected", port
  sht = SimpleHT(port)
  sht.register_introspection_functions()
  sht.register_function(sht.get)
  sht.register_function(sht.put)
  sht.register_function(sht.print_content)
  sht.register_function(sht.read_file)
  sht.register_function(sht.write_file)
  sht.register_function(sht.backup_from_peer)
  sht.register_function(sht.list_contents)
  sht.register_function(sht.corrupt)
  sht.register_function(sht.repair_corrupt)
  sht.register_function(sht.terminate)
  sht.serve_forever()
  print "Server shutting down"

# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port)

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    return self.caller.get(Binary(key))

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

class SimpleHTTest(unittest.TestCase):
  def test_direct(self):
    helper = Helper(SimpleHT())
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

    helper.write_file("test")
    helper = Helper(SimpleHT())

    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    helper.read_file("test")
    self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
    self.assertTrue(helper.put("some_other_key", "some_value", 10000))
    self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
    self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

  # Test via RPC
  def test_xmlrpc(self):
    output_thread = threading.Thread(target=serve_thread(), args=(51234, ))
    output_thread.setDaemon(True)
    output_thread.start()

    time.sleep(1)
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:51234"))
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

if __name__ == "__main__":
  main()
