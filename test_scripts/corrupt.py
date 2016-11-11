import logging
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from time import time
import datetime
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from xmlrpclib import Binary
import sys, pickle, xmlrpclib
from socket import error as socket_error
rpc = xmlrpclib.ServerProxy("http://localhost:51236")
rpc.list_contents()
rpc.corrupt('/a/you.txt&&data')
#rpc.terminate()     #to test that terminate is working
